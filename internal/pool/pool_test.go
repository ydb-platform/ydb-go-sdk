package pool

import (
	"context"
	"errors"
	"fmt"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	testItem struct {
		v int32

		closed bool

		onClose   func() error
		onIsAlive func() bool
		onNodeID  func() uint32
	}
	//	testWaitChPool struct {
	//		xsync.Pool[chan *testItem]
	//
	//		testHookGetWaitCh func()
	//	}
)

var defaultTrace = &Trace{
	OnNew: func(ctx *context.Context, call stack.Caller) func(limit int) {
		return func(limit int) {
		}
	},
	OnClose: func(ctx *context.Context, call stack.Caller) func(err error) {
		return func(err error) {
		}
	},
	OnTry: func(ctx *context.Context, call stack.Caller) func(err error) {
		return func(err error) {
		}
	},
	OnWith: func(ctx *context.Context, call stack.Caller) func(attempts int, err error) {
		return func(attempts int, err error) {
		}
	},
	OnPut: func(ctx *context.Context, call stack.Caller, info any) func(err error) {
		return func(err error) {
		}
	},
	OnGet: func(ctx *context.Context, call stack.Caller) func(
		info any,
		nodeHintInfo *trace.NodeHintInfo,
		err error,
	) {
		return func(info any, nodeHintInfo *trace.NodeHintInfo, err error) {
		}
	},
	onWait: func() func(info any, err error) {
		return func(info any, err error) {
		}
	},
	OnChange: func(stats Stats) {
	},
}

func (t *testItem) IsAlive() bool {
	if t.closed {
		return false
	}

	if t.onIsAlive != nil {
		return t.onIsAlive()
	}

	return true
}

func (t *testItem) ID() string {
	return ""
}

func (t *testItem) NodeID() uint32 {
	if t.onNodeID != nil {
		return t.onNodeID()
	}

	return 0
}

func (t *testItem) Close(context.Context) error {
	defer func() {
		t.closed = true
	}()

	if t.onClose != nil {
		return t.onClose()
	}

	return nil
}

func caller() string {
	_, file, line, _ := runtime.Caller(2)

	return fmt.Sprintf("%s:%d", path.Base(file), line)
}

func getItemWithFlush[PT ItemConstraint[T], T any](
	ctx context.Context,
	p *Pool[PT, T],
) (*itemInfo[PT, T], error) {
	var batchChanges dynamicStats
	defer p.applyBatchStats(&batchChanges)

	return p.getItem(ctx, &batchChanges)
}

func putItemWithFlush[PT ItemConstraint[T], T any](
	ctx context.Context,
	p *Pool[PT, T],
	info *itemInfo[PT, T],
) error {
	var batchChanges dynamicStats
	defer p.applyBatchStats(&batchChanges)

	return p.putItem(ctx, info, &batchChanges)
}

func mustGetItem[PT ItemConstraint[T], T any](t testing.TB, p *Pool[PT, T]) *itemInfo[PT, T] {
	t.Helper()

	info, err := getItemWithFlush(t.Context(), p)
	if err != nil {
		panic(err)
	}

	return info
}

func mustPutItem[PT ItemConstraint[T], T any](t testing.TB, p *Pool[PT, T], info *itemInfo[PT, T]) {
	t.Helper()

	if err := putItemWithFlush(t.Context(), p, info); err != nil {
		panic(err)
	}
}

func mustNewPool[PT ItemConstraint[T], T any](
	t testing.TB,
	opts ...Option[PT, T],
) *Pool[PT, T] {
	t.Helper()

	p, err := New(t.Context(), opts...)
	assert.NoError(t, err)

	return p
}

func mustClose(t testing.TB, pool closer.Closer) {
	if err := pool.Close(t.Context()); err != nil {
		panic(err)
	}
}

func poolStats(limit int, mutate func(*Stats)) Stats {
	s := Stats{Limit: limit}
	if mutate != nil {
		mutate(&s)
	}

	return s
}

func requirePoolStats(t testing.TB, p *Pool[*testItem, testItem], want Stats, msg ...any) {
	t.Helper()
	assert.Equal(t, want, p.Stats(), msg...)
}

func TestPool(t *testing.T) { //nolint:gocyclo
	t.Run("New", func(t *testing.T) {
		t.Run("Default", func(t *testing.T) {
			p := mustNewPool[*testItem, testItem](t,
				WithTrace[*testItem, testItem](defaultTrace),
			)
			requirePoolStats(t, p, poolStats(DefaultLimit, nil))

			err := p.With(t.Context(), func(ctx context.Context, testItem *testItem) error {
				assert.EqualValues(t, 0, testItem.NodeID())

				return nil
			})
			assert.NoError(t, err)
			requirePoolStats(t, p, poolStats(DefaultLimit, func(s *Stats) {
				s.Size = 1
				s.Idle = 1
			}))
		})
		t.Run("RequireNodeIdFromPool", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				hintTrace := defaultTrace
				var preferredID uint32
				hintTrace.OnGet = func(ctx *context.Context, call stack.Caller) func(
					info any,
					nodeHintInfo *trace.NodeHintInfo,
					err error,
				) {
					return func(info any, nodeHintInfo *trace.NodeHintInfo, err error) {
						if nodeHintInfo != nil {
							preferredID = nodeHintInfo.PreferredNodeID
						}
					}
				}
				var newItemCalled uint32
				p := mustNewPool[*testItem, testItem](t,
					WithTrace[*testItem, testItem](defaultTrace),
					WithCreateItemFunc(func(ctx context.Context) (*testItem, error) {
						newItemCalled++
						v := testItem{
							v: int32(newItemCalled),
							onNodeID: func() uint32 {
								nodeID, has := endpoint.ContextNodeID(ctx)
								if has {
									return nodeID
								}

								return 0
							},
						}

						return &v, nil
					}),
					WithLimit[*testItem, testItem](3),
				)
				requirePoolStats(t, p, poolStats(3, nil))

				info := mustGetItem(t, p)
				assert.EqualValues(t, 0, int(info.item.NodeID()))
				assert.EqualValues(t, true, info.item.IsAlive())
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 1 }))
				mustPutItem(t, p, info)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

				info, err := getItemWithFlush(endpoint.WithNodeID(t.Context(), 32), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 32, int(info.item.NodeID()))
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 2; s.Idle = 1 }))
				mustPutItem(t, p, info)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 2; s.Idle = 2 }))

				info, err = getItemWithFlush(endpoint.WithNodeID(t.Context(), 33), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 33, int(info.item.NodeID()))
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))
				mustPutItem(t, p, info)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 3 }))

				info, err = getItemWithFlush(endpoint.WithNodeID(t.Context(), 32), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 32, int(info.item.NodeID()))
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))
				mustPutItem(t, p, info)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 3 }))

				info, err = getItemWithFlush(endpoint.WithNodeID(t.Context(), 33), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 33, int(info.item.NodeID()))
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))
				mustPutItem(t, p, info)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 3 }))

				info, err = getItemWithFlush(endpoint.WithNodeID(t.Context(), 32), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 32, int(info.item.NodeID()))
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))
				info2, err := getItemWithFlush(endpoint.WithNodeID(t.Context(), 33), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 33, info2.item.NodeID())
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 1 }))
				mustPutItem(t, p, info2)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))
				mustPutItem(t, p, info)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 3 }))

				info, err = getItemWithFlush(endpoint.WithNodeID(t.Context(), 32), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 32, int(info.item.NodeID()))
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))
				info2, err = getItemWithFlush(endpoint.WithNodeID(t.Context(), 33), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 33, info2.item.NodeID())
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 1 }))
				mustPutItem(t, p, info)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))
				mustPutItem(t, p, info2)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 3 }))

				info, err = getItemWithFlush(endpoint.WithNodeID(t.Context(), 32), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 32, int(info.item.NodeID()))
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))
				info2, err = getItemWithFlush(endpoint.WithNodeID(t.Context(), 33), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 33, int(info2.item.NodeID()))
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 1 }))
				info3, err := getItemWithFlush(t.Context(), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 0, info3.item.NodeID())
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 0 }))
				mustPutItem(t, p, info)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 1 }))
				mustPutItem(t, p, info2)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))
				mustPutItem(t, p, info3)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 3 }))
				info, err = getItemWithFlush(endpoint.WithNodeID(t.Context(), 100), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 100, preferredID)
				assert.EqualValues(t, 100, int(info.item.NodeID()))
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 3 }))
				assert.EqualValues(t, 4, int(newItemCalled))
				_, _ = getItemWithFlush(endpoint.WithNodeID(t.Context(), 32), p)
				stats := p.Stats()
				assert.Equal(t, 3, stats.Limit)
				assert.LessOrEqual(t, stats.Idle, 3)
				assert.GreaterOrEqual(t, stats.Size, 3)
				_, _ = getItemWithFlush(endpoint.WithNodeID(t.Context(), 32), p)
				stats = p.Stats()
				assert.Equal(t, 3, stats.Limit)
				assert.LessOrEqual(t, stats.Idle, 3)
				assert.GreaterOrEqual(t, stats.Size, 3)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				// should not panic
				_, _ = getItemWithFlush(endpoint.WithNodeID(ctx, 32), p)
				stats = p.Stats()
				assert.Equal(t, 3, stats.Limit)
				assert.LessOrEqual(t, stats.Idle, 3)
			})
		})
		t.Run("PreferredNodeTakenAndPoolFull", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				p := mustNewPool[*testItem, testItem](t,
					WithTrace[*testItem, testItem](defaultTrace),
					WithCreateItemFunc(func(ctx context.Context) (*testItem, error) {
						v := testItem{
							v: 0,
							onNodeID: func() uint32 {
								nodeID, has := endpoint.ContextNodeID(ctx)
								if !has {
									nodeID = 0
								}

								return nodeID
							},
						}

						return &v, nil
					}),
					WithLimit[*testItem, testItem](2),
				)
				requirePoolStats(t, p, poolStats(2, nil))

				info1, err := getItemWithFlush(endpoint.WithNodeID(t.Context(), 32), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 32, info1.item.NodeID())
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 1 }))
				info2, err := getItemWithFlush(endpoint.WithNodeID(t.Context(), 33), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 33, info2.item.NodeID())
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 2 }))

				mustPutItem(t, p, info2)
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 2; s.Idle = 1 }))

				info3, err := getItemWithFlush(endpoint.WithNodeID(t.Context(), 32), p)
				assert.NoError(t, err)
				assert.EqualValues(t, 32, info3.item.NodeID())
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 2; s.Idle = 1 }))
			})
		})
		t.Run("CreateItemOnGivenNode", func(t *testing.T) {
			var newItemCalled uint32
			p := mustNewPool[*testItem, testItem](t,
				WithTrace[*testItem, testItem](defaultTrace),
				WithCreateItemFunc(func(ctx context.Context) (*testItem, error) {
					newItemCalled++
					v := testItem{
						v: 0,
						onNodeID: func() uint32 {
							nodeID, _ := endpoint.ContextNodeID(ctx)

							return nodeID
						},
					}

					return &v, nil
				}),
			)
			requirePoolStats(t, p, poolStats(DefaultLimit, nil))

			info, err := getItemWithFlush(endpoint.WithNodeID(t.Context(), 32), p)
			assert.NoError(t, err)
			assert.NotNil(t, info)
			assert.NotNil(t, info.item)
			assert.EqualValues(t, 32, int(info.item.NodeID()))
			assert.EqualValues(t, true, info.item.IsAlive())
			requirePoolStats(t, p, poolStats(DefaultLimit, func(s *Stats) { s.Size = 1 }))
			mustPutItem(t, p, info)
			requirePoolStats(t, p, poolStats(DefaultLimit, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

			info = mustGetItem(t, p)
			assert.EqualValues(t, 32, int(info.item.NodeID()))
			requirePoolStats(t, p, poolStats(DefaultLimit, func(s *Stats) { s.Size = 1 }))
			mustPutItem(t, p, info)
			requirePoolStats(t, p, poolStats(DefaultLimit, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

			assert.EqualValues(t, 1, newItemCalled)
		})
		t.Run("WithLimit", func(t *testing.T) {
			p := mustNewPool[*testItem, testItem](t, WithLimit[*testItem, testItem](1),
				WithTrace[*testItem, testItem](defaultTrace),
			)
			assert.EqualValues(t, 1, p.config.limit)
			requirePoolStats(t, p, poolStats(1, nil))
		})
		t.Run("WithItemUsageLimit", func(t *testing.T) {
			var (
				newCounter        int64
				errMustDeleteItem = xerrors.Retryable(errors.New("test"))
			)
			p := mustNewPool[*testItem, testItem](t,
				WithLimit[*testItem, testItem](1),
				WithItemUsageLimit[*testItem, testItem](5),
				WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					atomic.AddInt64(&newCounter, 1)

					var v testItem

					return &v, nil
				}),
				WithMustDeleteItemFunc[*testItem, testItem](func(item *testItem, err error) bool {
					return !item.IsAlive() || xerrors.Is(err, errMustDeleteItem)
				}),
			)
			assert.EqualValues(t, 1, p.config.limit)
			requirePoolStats(t, p, poolStats(1, nil))
			var lambdaCounter atomic.Int64
			err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
				if lambdaCounter.Add(1) < 10 {
					return xerrors.WithStackTrace(errMustDeleteItem)
				}

				return nil
			})
			assert.NoError(t, err)
			assert.EqualValues(t, 10, newCounter)
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))
		})
		t.Run("WithCreateItemFunc", func(t *testing.T) {
			var newCounter atomic.Int64
			p := mustNewPool(t,
				WithLimit[*testItem, testItem](1),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					newCounter.Add(1)
					var v testItem

					return &v, nil
				}),
				WithTrace[*testItem, testItem](defaultTrace),
			)
			requirePoolStats(t, p, poolStats(1, nil))
			err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
				return nil
			})
			assert.NoError(t, err)
			assert.EqualValues(t, p.config.limit, newCounter.Load())
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))
		})
	})
	t.Run("Close", func(t *testing.T) {
		counter := 0
		xtest.TestManyTimes(t, func(t testing.TB) {
			counter++
			defer func() {
				if counter%1000 == 0 {
					t.Logf("%d times test passed", counter)
				}
			}()

			var (
				created atomic.Int32
				closed  = [...]bool{false, false, false}
			)

			p := mustNewPool[*testItem, testItem](t,
				WithLimit[*testItem, testItem](3),
				WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					var (
						idx = created.Add(1) - 1
						v   = testItem{
							v: 0,
							onClose: func() error {
								closed[idx] = true

								return nil
							},
						}
					)

					return &v, nil
				}),
				WithTrace[*testItem, testItem](defaultTrace),
			)
			requirePoolStats(t, p, poolStats(3, nil))

			defer func() {
				_ = p.Close(t.Context())
			}()

			var (
				s1 = mustGetItem(t, p)
				s2 = mustGetItem(t, p)
				s3 = mustGetItem(t, p)
			)
			requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3 }))

			mustPutItem(t, p, s1)
			mustPutItem(t, p, s2)
			requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))

			mustClose(t, p)
			// Close drains idle via PopAll with decrementing Idle stat; s3 is still held
			requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 1; s.Idle = 0 }))

			assert.True(t, closed[0])  // idle info in pool
			assert.True(t, closed[1])  // idle info in pool
			assert.False(t, closed[2]) // info extracted from idle but closed later on putItem

			assert.ErrorIs(t, putItemWithFlush(t.Context(), p, s3), errClosedPool)
			requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 0; s.Idle = 0 }))

			assert.True(t, closed[2]) // after putItem s3 must be closed
		})
		t.Run("WithCancelledContext", func(t *testing.T) {
			// Regression test: closing idle infos from the pool must succeed even
			// when the context passed to Close is already cancelled.
			xtest.TestManyTimes(t, func(t testing.TB) {
				var closedCount atomic.Int32

				p := mustNewPool[*testItem, testItem](t,
					WithLimit[*testItem, testItem](3),
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						var v testItem

						return &v, nil
					}),
					WithTrace[*testItem, testItem](defaultTrace),
				)
				requirePoolStats(t, p, poolStats(3, nil))

				// Override the close func to detect context-cancelled failures.
				// In a real scenario (e.g. gRPC session close), a cancelled context
				// would cause the close call to fail immediately.
				p.config.closeItemFunc = func(ctx context.Context, info *testItem) {
					closedCount.Add(1)
				}

				info1 := mustGetItem(t, p)
				info2 := mustGetItem(t, p)
				info3 := mustGetItem(t, p)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3 }))

				mustPutItem(t, p, info1)
				mustPutItem(t, p, info2)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))

				// Close the pool with an already-cancelled context.
				cancelCtx, cancel := context.WithCancel(t.Context())
				cancel()

				err := p.Close(cancelCtx)
				assert.NoError(t, err)
				requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 1; s.Idle = 0 }))

				// Both idle infos must have been closed despite the cancelled context.
				assert.Equal(t, int32(2), closedCount.Load())

				_ = info3 // info3 is still "in use" and not in the idle list
			})
		})
		t.Run("WhenWaiting", func(t *testing.T) {
			const timeout = time.Second

			waitPoolClosed := func(t *testing.T, ch <-chan error) {
				t.Helper()
				select {
				case err := <-ch:
					assert.NoError(t, err)
				case <-time.After(timeout):
					t.Fatalf("pool.Close did not finish within %s", timeout)
				}
			}

			waitWithClosed := func(t *testing.T, ch <-chan error) {
				t.Helper()
				select {
				case err := <-ch:
					assert.ErrorIs(t, err, errClosedPool)
				case <-time.After(timeout):
					t.Fatalf("pool.With did not finish within %s", timeout)
				}
			}

			holdInWith := func(
				ctx context.Context,
				p *Pool[*testItem, testItem],
				ready chan<- struct{},
				release <-chan struct{},
			) error {
				return p.With(ctx, func(context.Context, *testItem) error {
					close(ready)
					<-release

					return nil
				})
			}

			t.Run("WithBlockedOnSemaphore", func(t *testing.T) {
				// All semaphore slots are taken inside active pool.With callbacks;
				// another pool.With blocks on acquiring a slot until pool.Close.
				ctx := t.Context()
				p := mustNewPool[*testItem, testItem](t,
					WithLimit[*testItem, testItem](2),
					WithTrace[*testItem, testItem](defaultTrace),
				)
				requirePoolStats(t, p, poolStats(2, nil))

				release1 := make(chan struct{})
				release2 := make(chan struct{})
				ready1 := make(chan struct{})
				ready2 := make(chan struct{})

				go func() { _ = holdInWith(ctx, p, ready1, release1) }()
				go func() { _ = holdInWith(ctx, p, ready2, release2) }()
				<-ready1
				<-ready2

				waiting := make(chan error, 1)
				go func() {
					waiting <- p.With(ctx, func(context.Context, *testItem) error {
						return nil
					})
				}()

				closed := make(chan error, 1)
				go func() {
					closed <- p.Close(ctx)
				}()

				waitWithClosed(t, waiting)

				close(release1)
				close(release2)
				waitPoolClosed(t, closed)
			})

			t.Run("CloseAfterClose", func(t *testing.T) {
				ctx := t.Context()
				p := mustNewPool[*testItem, testItem](t,
					WithLimit[*testItem, testItem](1),
					WithTrace[*testItem, testItem](defaultTrace),
				)
				requirePoolStats(t, p, poolStats(1, nil))

				assert.NoError(t, p.Close(ctx))
				requirePoolStats(t, p, poolStats(1, nil))

				err := p.With(ctx, func(context.Context, *testItem) error {
					return nil
				})
				assert.ErrorIs(t, err, errClosedPool)
			})

			t.Run("CloseWhileTryBeforeSemaphoreAcquire", func(t *testing.T) {
				// pool.With entered try (OnTry fired) but has not taken a semaphore slot yet;
				// analog of the old "racy" wait-queue registration case.
				ctx := t.Context()
				tryStarted := make(chan struct{})
				trace := *defaultTrace
				trace.OnTry = func(ctx *context.Context, call stack.Caller) func(err error) {
					select {
					case tryStarted <- struct{}{}:
					default:
					}

					return func(err error) {}
				}

				p := mustNewPool[*testItem, testItem](t,
					WithLimit[*testItem, testItem](2),
					WithTrace[*testItem, testItem](&trace),
				)
				requirePoolStats(t, p, poolStats(2, nil))

				release1 := make(chan struct{})
				release2 := make(chan struct{})
				ready1 := make(chan struct{})
				ready2 := make(chan struct{})

				go func() { _ = holdInWith(ctx, p, ready1, release1) }()
				go func() { _ = holdInWith(ctx, p, ready2, release2) }()
				<-ready1
				<-ready2

				waiting := make(chan error, 1)
				go func() {
					waiting <- p.With(ctx, func(context.Context, *testItem) error {
						return nil
					})
				}()
				<-tryStarted

				closed := make(chan error, 1)
				go func() {
					closed <- p.Close(ctx)
				}()

				waitWithClosed(t, waiting)

				close(release1)
				close(release2)
				waitPoolClosed(t, closed)
			})
			t.Run("CloseWhileWithInCallback", func(t *testing.T) {
				// pool.Close waits until an in-flight pool.With releases its slot.
				ctx := t.Context()
				p := mustNewPool[*testItem, testItem](t,
					WithLimit[*testItem, testItem](1),
					WithTrace[*testItem, testItem](defaultTrace),
				)
				requirePoolStats(t, p, poolStats(1, nil))

				release := make(chan struct{})
				ready := make(chan struct{})
				closed := make(chan error, 1)

				go func() {
					_ = holdInWith(ctx, p, ready, release)
				}()
				<-ready

				go func() {
					closed <- p.Close(ctx)
				}()

				select {
				case <-closed:
					t.Fatal("pool.Close returned before active pool.With released its slot")
				case <-time.After(50 * time.Millisecond):
				}

				close(release)
				waitPoolClosed(t, closed)
				requirePoolStats(t, p, poolStats(1, nil))
			})

			t.Run("NoSendOnClosedSemaphore", func(t *testing.T) {
				// A semaphore slot is acquired only in pool.try (<-p.sema with ok == true)
				// and drained in pool.Close. pool.Close closes p.sema only after all slots
				// are returned, so the release in try's defer must never send on a closed channel.
				xtest.TestManyTimes(t, func(t testing.TB) {
					const limit = 4

					ctx := t.Context()
					p := mustNewPool[*testItem, testItem](t,
						WithLimit[*testItem, testItem](limit),
						WithTrace[*testItem, testItem](defaultTrace),
					)
					requirePoolStats(t, p, poolStats(limit, nil))

					panics := make(chan any, 1)
					goWithPanicGuard := func(wg *sync.WaitGroup, fn func()) {
						wg.Add(1)
						go func() {
							defer wg.Done()
							defer func() {
								if v := recover(); v != nil {
									select {
									case panics <- v:
									default:
									}
								}
							}()
							fn()
						}()
					}

					var wg sync.WaitGroup

					release := make(chan struct{})
					holdersReady := make(chan struct{}, limit)

					for range limit {
						goWithPanicGuard(&wg, func() {
							_ = p.With(ctx, func(context.Context, *testItem) error {
								holdersReady <- struct{}{}
								<-release

								return nil
							})
						})
					}

					for range limit {
						<-holdersReady
					}

					goWithPanicGuard(&wg, func() {
						_ = p.Close(ctx)
					})

					for range limit * 4 {
						goWithPanicGuard(&wg, func() {
							_ = p.With(ctx, func(context.Context, *testItem) error {
								return nil
							})
						})
					}

					close(release)

					done := make(chan struct{})
					go func() {
						wg.Wait()
						close(done)
					}()

					select {
					case v := <-panics:
						t.Fatalf("panic: %v (possible send on closed semaphore)", v)
					case <-done:
					case <-time.After(timeout):
						t.Fatalf("test timed out after %s", timeout)
					}

					select {
					case v := <-panics:
						t.Fatalf("panic: %v (possible send on closed semaphore)", v)
					default:
					}
				})
			})
		})
		t.Run("IdleItems", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					created       atomic.Int32
					idleThreshold = 4 * time.Second
					closedCount   atomic.Int64
					fakeClock     = clockwork.NewFakeClock()
				)
				p := mustNewPool[*testItem, testItem](t,
					WithLimit[*testItem, testItem](2),
					WithCreateItemTimeout[*testItem, testItem](0),
					WithCreateItemFunc[*testItem, testItem](func(ctx context.Context) (*testItem, error) {
						v := testItem{
							v: created.Add(1),
							onClose: func() error {
								closedCount.Add(1)

								return nil
							},
						}

						return &v, nil
					}),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithClock[*testItem, testItem](fakeClock),
					WithIdleTimeToLive[*testItem, testItem](idleThreshold),
					WithTrace[*testItem, testItem](defaultTrace),
				)
				requirePoolStats(t, p, poolStats(2, nil))

				info1 := mustGetItem(t, p)
				info2 := mustGetItem(t, p)
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 2 }))

				// Put both items at the absolutely same time.
				// That is, both items must be updated their lastUsage timestamp.
				mustPutItem(t, p, info1)
				mustPutItem(t, p, info2)
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 2; s.Idle = 2 }))

				// Move clock to longer than idleTimeToLive
				fakeClock.Advance(idleThreshold + time.Nanosecond)

				// on get info from idle list the pool must check the info idle timestamp
				// both existing infos must be closed
				// getItem must create a new info and return it from getItem
				info3 := mustGetItem(t, p)
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 1 }))

				if !closedCount.CompareAndSwap(2, 0) {
					t.Fatal("unexpected number of closed info's")
				}

				// Move time to idleTimeToLive / 2 - this emulate a "spent" some time working within info.
				fakeClock.Advance(idleThreshold / 2)

				// Now put that info back
				// pool must update a lastUsage timestamp of info
				mustPutItem(t, p, info3)
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

				// Move time to idleTimeToLive / 2
				// Total time since last updating lastUsage timestampe is more than idleTimeToLive
				fakeClock.Advance(idleThreshold/2 + time.Nanosecond)

				info4 := mustGetItem(t, p)
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 1 }))
				mustPutItem(t, p, info4)
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

				_ = p.Close(t.Context())
				requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Idle = 0 }))
				assert.Equal(t, 0, p.Stats().Size)
			})
		})
	})
	t.Run("Retry", func(t *testing.T) {
		t.Run("CreateItem", func(t *testing.T) {
			t.Run("context", func(t *testing.T) {
				for _, tt := range []struct {
					name string
					err  error
				}{
					{
						name: "Canceled",
						err:  context.Canceled,
					},
					{
						name: "DeadlineExceeded",
						err:  context.DeadlineExceeded,
					},
				} {
					t.Run(tt.name, func(t *testing.T) {
						var counter atomic.Int64
						p := mustNewPool(t,
							WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
							WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
							WithCreateItemFunc(func(context.Context) (*testItem, error) {
								counter.Add(1)

								if counter.Load() < 10 {
									return nil, tt.err
								}

								var v testItem

								return &v, nil
							}),
						)
						requirePoolStats(t, p, poolStats(DefaultLimit, nil))
						err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
							return nil
						})
						assert.NoError(t, err)
						assert.GreaterOrEqual(t, counter.Load(), int64(10))
						requirePoolStats(t, p, poolStats(DefaultLimit, func(s *Stats) {
							s.Size = 1
							s.Idle = 1
						}))
					})
				}
			})
			t.Run("OnTransportError", func(t *testing.T) {
				var counter atomic.Int64
				p := mustNewPool(t,
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						counter.Add(1)

						if counter.Load() < 10 {
							return nil, xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
						}

						var v testItem

						return &v, nil
					}),
				)
				requirePoolStats(t, p, poolStats(DefaultLimit, nil))
				err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
					return nil
				})
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, counter.Load(), int64(10))
				requirePoolStats(t, p, poolStats(DefaultLimit, func(s *Stats) {
					s.Size = 1
					s.Idle = 1
				}))
			})
			t.Run("OnOperationError", func(t *testing.T) {
				var counter atomic.Int64
				p := mustNewPool(t,
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						counter.Add(1)

						if counter.Load() < 10 {
							return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE))
						}

						var v testItem

						return &v, nil
					}),
				)
				requirePoolStats(t, p, poolStats(DefaultLimit, nil))
				err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
					return nil
				})
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, counter.Load(), int64(10))
				requirePoolStats(t, p, poolStats(DefaultLimit, func(s *Stats) {
					s.Size = 1
					s.Idle = 1
				}))
			})
			t.Run("OnUnauthorized", func(t *testing.T) {
				unauthorized := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED))

				var createAttempts atomic.Int64
				p := mustNewPool[*testItem, testItem](t,
					WithLimit[*testItem, testItem](1),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						createAttempts.Add(1)

						return nil, unauthorized
					}),
				)
				requirePoolStats(t, p, poolStats(1, nil))

				var callbackRuns int
				err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
					callbackRuns++

					return nil
				})
				assert.Error(t, err)
				assert.Equal(t, 0, callbackRuns)
				assert.Equal(t, int64(1), createAttempts.Load())
				assert.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAUTHORIZED))
				assert.Nil(t, xerrors.RetryableError(err))
				requirePoolStats(t, p, poolStats(1, nil))
			})
			t.Run("NilNil", func(t *testing.T) {
				xtest.TestManyTimes(t, func(t testing.TB) {
					limit := 100
					ctx, cancel := xcontext.WithTimeout(
						t.Context(),
						55*time.Second,
					)
					defer cancel()
					p := mustNewPool[*testItem, testItem](t)
					requirePoolStats(t, p, poolStats(DefaultLimit, nil))
					defer func() {
						_ = p.Close(t.Context())
					}()
					r := xrand.New(xrand.WithLock())
					errCh := make(chan error, limit*10)
					fn := func(wg *sync.WaitGroup) {
						defer wg.Done()
						childCtx, childCancel := xcontext.WithTimeout(
							ctx,
							time.Duration(r.Int64(int64(time.Second))),
						)
						defer childCancel()
						var batchChanges dynamicStats
						defer p.applyBatchStats(&batchChanges)

						s, err := p.createItem(childCtx, &batchChanges)
						if s == nil && err == nil {
							errCh <- fmt.Errorf("unexpected result: <%v, %w>", s, err)
						}
					}
					wg := &sync.WaitGroup{}
					wg.Add(limit * 10)
					for i := 0; i < limit*10; i++ {
						go fn(wg)
					}
					go func() {
						wg.Wait()
						close(errCh)
					}()
					for e := range errCh {
						t.Fatal(e)
					}
				})
			})
		})
		t.Run("On", func(t *testing.T) {
			t.Run("Context", func(t *testing.T) {
				t.Run("Canceled", func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					cancel()
					p, err := New(ctx, WithLimit[*testItem, testItem](1))
					assert.NoError(t, err)
					requirePoolStats(t, p, poolStats(1, nil))
					err = p.With(ctx, func(ctx context.Context, testItem *testItem) error {
						return nil
					})
					assert.ErrorIs(t, err, context.Canceled)
					requirePoolStats(t, p, poolStats(1, nil))
				})
				t.Run("DeadlineExceeded", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 0)
					cancel()
					p, err := New(ctx, WithLimit[*testItem, testItem](1))
					assert.NoError(t, err)
					requirePoolStats(t, p, poolStats(1, nil))
					err = p.With(ctx, func(ctx context.Context, testItem *testItem) error {
						return nil
					})
					assert.ErrorIs(t, err, context.DeadlineExceeded)
					requirePoolStats(t, p, poolStats(1, nil))
				})
			})
		})
		t.Run("DoBackoffRetryCancelation", func(t *testing.T) {
			for _, testErr := range []error{
				// Errors leading to Wait repeat.
				xerrors.Transport(
					grpcStatus.Error(grpcCodes.ResourceExhausted, ""),
				),
				fmt.Errorf("wrap transport error: %w", xerrors.Transport(
					grpcStatus.Error(grpcCodes.ResourceExhausted, ""),
				)),
				xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)),
				fmt.Errorf("wrap op error: %w", xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))),
			} {
				t.Run(testErr.Error(), func(t *testing.T) {
					backoff := make(chan chan time.Time)
					ctx, cancel := xcontext.WithCancel(t.Context())
					p, err := New(ctx, WithLimit[*testItem, testItem](1))
					assert.NoError(t, err)
					requirePoolStats(t, p, poolStats(1, nil))

					results := make(chan error)
					go func() {
						err := p.With(ctx,
							func(ctx context.Context, info *testItem) error {
								return testErr
							},
							retry.WithFastBackoff(
								testutil.BackoffFunc(func(n int) <-chan time.Time {
									ch := make(chan time.Time)
									backoff <- ch

									return ch
								}),
							),
							retry.WithSlowBackoff(
								testutil.BackoffFunc(func(n int) <-chan time.Time {
									ch := make(chan time.Time)
									backoff <- ch

									return ch
								}),
							),
						)
						results <- err
					}()

					select {
					case <-backoff:
						t.Logf("expected result")
					case res := <-results:
						t.Fatalf("unexpected result: %v", res)
					}

					cancel()
				})
			}
		})
	})
	t.Run("Item", func(t *testing.T) {
		t.Run("Close", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					createCounter atomic.Int64
					closeCounter  atomic.Int64
				)
				p := mustNewPool(t,
					WithLimit[*testItem, testItem](1),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						createCounter.Add(1)

						v := &testItem{
							onClose: func() error {
								closeCounter.Add(1)

								return nil
							},
						}

						return v, nil
					}),
				)
				requirePoolStats(t, p, poolStats(1, nil))
				err := p.With(t.Context(), func(ctx context.Context, testItem *testItem) error {
					return nil
				})
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, createCounter.Load(), closeCounter.Load())
				requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))
				err = p.Close(t.Context())
				assert.NoError(t, err)
				assert.EqualValues(t, createCounter.Load(), closeCounter.Load())
				// Close drains idle with decrementing Idle stat
				requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Idle = 0 }))
				assert.Equal(t, 0, p.Stats().Size)
			})
		})
		t.Run("IsAlive", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					newItems    atomic.Int64
					deleteItems atomic.Int64
					expErr      = errors.New("expected error")
				)
				p := mustNewPool(t,
					WithLimit[*testItem, testItem](1),
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						newItems.Add(1)

						v := &testItem{
							onClose: func() error {
								deleteItems.Add(1)

								return nil
							},
							onIsAlive: func() bool {
								return newItems.Load() >= 10
							},
						}

						return v, nil
					}),
				)
				requirePoolStats(t, p, poolStats(1, nil))
				err := p.With(t.Context(), func(ctx context.Context, testItem *testItem) error {
					if newItems.Load() < 10 {
						return xerrors.Retryable(expErr, xerrors.Invalid(testItem))
					}

					return nil
				})
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, newItems.Load(), int64(9))
				assert.GreaterOrEqual(t, newItems.Load(), deleteItems.Load())
				requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))
				err = p.Close(t.Context())
				assert.NoError(t, err)
				assert.EqualValues(t, newItems.Load(), deleteItems.Load())
				requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Idle = 0 }))
			})
		})
	})
	t.Run("With", func(t *testing.T) {
		t.Run("ItemUsageLimit", func(t *testing.T) {
			const usageLimit uint64 = 3

			ctx := t.Context()
			var (
				created atomic.Int32
				closed  atomic.Int32
			)

			p := mustNewPool[*testItem, testItem](t,
				WithLimit[*testItem, testItem](1),
				WithItemUsageLimit[*testItem, testItem](usageLimit),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					id := created.Add(1)

					return &testItem{
						v: id,
						onClose: func() error {
							closed.Add(1)

							return nil
						},
					}, nil
				}),
			)
			defer func() {
				_ = p.Close(ctx)
			}()
			requirePoolStats(t, p, poolStats(1, nil))

			for range usageLimit {
				err := p.With(ctx, func(context.Context, *testItem) error {
					return nil
				})
				assert.NoError(t, err)
			}
			assert.Equal(t, int32(1), created.Load())
			assert.Equal(t, int32(0), closed.Load())
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

			err := p.With(ctx, func(_ context.Context, item *testItem) error {
				assert.Equal(t, int32(2), item.v, "item must be recreated after usage limit is reached")

				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, int32(2), created.Load())
			assert.Equal(t, int32(1), closed.Load())
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))
		})
		t.Run("ItemUsageTTL", func(t *testing.T) {
			const usageTTL = time.Hour

			ctx := t.Context()
			var (
				created   atomic.Int32
				closed    atomic.Int32
				fakeClock = clockwork.NewFakeClock()
			)

			p := mustNewPool[*testItem, testItem](t,
				WithLimit[*testItem, testItem](1),
				WithItemUsageTTL[*testItem, testItem](usageTTL),
				WithClock[*testItem, testItem](fakeClock),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					id := created.Add(1)

					return &testItem{
						v: id,
						onClose: func() error {
							closed.Add(1)

							return nil
						},
					}, nil
				}),
			)
			defer func() {
				_ = p.Close(ctx)
			}()
			requirePoolStats(t, p, poolStats(1, nil))

			err := p.With(ctx, func(context.Context, *testItem) error {
				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, int32(1), created.Load())
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

			fakeClock.Advance(usageTTL + time.Nanosecond)

			err = p.With(ctx, func(_ context.Context, item *testItem) error {
				assert.Equal(t, int32(2), item.v, "item must be recreated after item usage TTL")

				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, int32(2), created.Load())
			assert.Equal(t, int32(1), closed.Load())
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))
		})
		t.Run("SpoiledIdleItems", func(t *testing.T) {
			// Models query sessions that become !IsAlive() while sitting in idle
			// (e.g. attach stream Recv error in listenAttachStream).
			ctx := t.Context()

			var (
				created atomic.Int32
				closed  atomic.Int32
				spoiled sync.Map // item id -> struct{}
			)

			p := mustNewPool[*testItem, testItem](t,
				WithLimit[*testItem, testItem](2),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					id := created.Add(1)

					return &testItem{
						v: id,
						onIsAlive: func() bool {
							_, dead := spoiled.Load(id)

							return !dead
						},
						onClose: func() error {
							closed.Add(1)

							return nil
						},
					}, nil
				}),
				WithTrace[*testItem, testItem](defaultTrace),
			)
			defer func() {
				_ = p.Close(ctx)
			}()
			requirePoolStats(t, p, poolStats(2, nil))

			err := p.With(ctx, func(context.Context, *testItem) error {
				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, int32(1), created.Load())
			assert.Equal(t, int32(0), closed.Load())
			requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

			spoiled.Store(int32(1), struct{}{})

			var gotID int32
			err = p.With(ctx, func(_ context.Context, item *testItem) error {
				gotID = item.v

				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, int32(2), gotID, "must not reuse spoiled idle item")
			assert.Equal(t, int32(2), created.Load())
			assert.Equal(t, int32(1), closed.Load(), "spoiled idle item must be closed on get")
			requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

			spoiled.Store(int32(2), struct{}{})

			err = p.With(ctx, func(_ context.Context, item *testItem) error {
				gotID = item.v

				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, int32(3), gotID)
			assert.Equal(t, int32(3), created.Load())
			assert.Equal(t, int32(2), closed.Load())
			requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 1; s.Idle = 1 }))
		})
		t.Run("ItemFromPoolIsNotAlive", func(t *testing.T) {
			var (
				created atomic.Int32
				closed  atomic.Int32
				nextID  atomic.Int32
			)
			assertCreated := func(exp int32) {
				if act := created.Load(); act != exp {
					t.Helper()
					t.Errorf(
						"unexpected number of created items: %v; want %v",
						act, exp,
					)
				}
			}
			assertClosed := func(exp int32) {
				if act := closed.Load(); act != exp {
					t.Helper()
					t.Errorf(
						"unexpected number of closed items: %v; want %v",
						act, exp,
					)
				}
			}
			p := mustNewPool[*testItem, testItem](t,
				WithLimit[*testItem, testItem](1),
				WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					created.Add(1)
					alived := true
					v := testItem{
						v: nextID.Add(1),
						onIsAlive: func() bool {
							defer func() {
								alived = false
							}()

							return alived
						},
						onClose: func() error {
							closed.Add(1)

							return nil
						},
					}

					return &v, nil
				}),
			)
			defer func() {
				_ = p.Close(t.Context())
			}()
			requirePoolStats(t, p, poolStats(1, nil))

			info1 := mustGetItem(t, p)
			assertClosed(0)
			assertCreated(1)
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1 }))

			mustPutItem(t, p, info1)
			assertClosed(0)
			assertCreated(1)
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

			info2, err := getItemWithFlush(t.Context(), p)
			assert.NoError(t, err)
			assertCreated(1)
			assertClosed(0)
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1 }))

			_, err = getItemWithFlush(t.Context(), p)
			assert.NoError(t, err)
			assertCreated(2)
			assertClosed(0)
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 2 }))

			assert.NoError(t, p.Close(t.Context()))
			assertCreated(2)
			assertClosed(0)
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 2 }))

			assert.ErrorIs(t, putItemWithFlush(t.Context(), p, info2), errClosedPool)
			assertClosed(1)

			assert.True(t, info2.item.closed)
			assert.False(t, info2.item.IsAlive())
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1 }))
		})
		t.Run("ExplicitItemClose", func(t *testing.T) {
			var (
				created atomic.Int32
				closed  atomic.Int32
			)
			assertCreated := func(exp int32) {
				if act := created.Load(); act != exp {
					t.Errorf(
						"unexpected number of created infos: %v; want %v",
						act, exp,
					)
				}
			}
			assertClosed := func(exp int32) {
				if act := closed.Load(); act != exp {
					t.Errorf(
						"unexpected number of closed infos: %v; want %v",
						act, exp,
					)
				}
			}
			p := mustNewPool[*testItem, testItem](t,
				WithLimit[*testItem, testItem](1),
				WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					created.Add(1)
					v := testItem{
						v: 0,
						onClose: func() error {
							closed.Add(1)

							return nil
						},
					}

					return &v, nil
				}),
			)
			defer func() {
				_ = p.Close(t.Context())
			}()
			requirePoolStats(t, p, poolStats(1, nil))

			info := mustGetItem(t, p)
			assertCreated(1)
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1 }))

			mustPutItem(t, p, info)
			assertClosed(0)
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))

			mustGetItem(t, p)
			assertCreated(1)
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1 }))

			info.item.Close(t.Context())

			assertClosed(1)
			// explicit item.Close does not update pool Size
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1 }))

			mustGetItem(t, p)
			assertCreated(2)
			requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 2 }))
		})
		t.Run("OnErrorWithDeleteItem", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					created           atomic.Int32
					errMustDeleteItem = errors.New("info must be deleted")
				)
				p := mustNewPool[*testItem, testItem](t,
					WithLimit[*testItem, testItem](1),
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						v := testItem{
							v: created.Add(1),
						}

						return &v, nil
					}),
					WithMustDeleteItemFunc[*testItem, testItem](func(info *testItem, err error) bool {
						return errors.Is(err, errMustDeleteItem)
					}),
				)
				defer func() {
					_ = p.Close(t.Context())
				}()
				requirePoolStats(t, p, poolStats(1, nil))

				errThrown := false
				err := p.With(t.Context(), func(ctx context.Context, testItem *testItem) error {
					if errThrown {
						assert.EqualValues(t, 2, testItem.v)

						return nil
					}

					assert.EqualValues(t, 1, testItem.v)

					defer func() {
						errThrown = true
					}()

					return xerrors.Retryable(errMustDeleteItem)
				})

				assert.NoError(t, err)
				requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))
			})
		})
		t.Run("Racy", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				trace := &Trace{
					OnChange: func(stats Stats) {
						assert.GreaterOrEqual(t, stats.Limit, stats.Idle)
					},
				}
				p := mustNewPool[*testItem, testItem](t,
					WithTrace[*testItem, testItem](trace),
				)
				requirePoolStats(t, p, poolStats(DefaultLimit, nil))
				r := xrand.New(xrand.WithLock())
				var wg sync.WaitGroup
				wg.Add(DefaultLimit*2 + 1)
				for range make([]struct{}, DefaultLimit*2) {
					go func() {
						defer wg.Done()
						childCtx, childCancel := xcontext.WithTimeout(
							t.Context(),
							time.Duration(r.Int64(int64(time.Second))),
						)
						defer childCancel()
						err := p.With(childCtx, func(ctx context.Context, testItem *testItem) error {
							return nil
						})
						if err != nil && !xerrors.Is(err, errClosedPool, context.Canceled) {
							t.Failed()
						}
					}()
				}
				go func() {
					defer wg.Done()
					time.Sleep(time.Millisecond)
					err := p.Close(t.Context())
					assert.NoError(t, err)
				}()
				wg.Wait()
			})
		})
		t.Run("ParallelCreation", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				trace := &Trace{
					OnChange: func(stats Stats) {
						assert.Equal(t, DefaultLimit, stats.Limit)
						assert.LessOrEqual(t, stats.Idle, DefaultLimit)
					},
				}
				p := mustNewPool[*testItem, testItem](t,
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithTrace[*testItem, testItem](trace),
				)
				requirePoolStats(t, p, poolStats(DefaultLimit, nil))
				var wg sync.WaitGroup
				for range make([]struct{}, DefaultLimit*10) {
					wg.Add(1)
					go func() {
						defer wg.Done()
						err := p.With(t.Context(), func(ctx context.Context, testItem *testItem) error {
							return nil
						})
						if err != nil && !xerrors.Is(err, errClosedPool, context.Canceled) {
							t.Failed()
						}
						stats := p.Stats()
						assert.LessOrEqual(t, stats.Idle, DefaultLimit)
					}()
				}

				wg.Wait()
			})
		})
	})
	t.Run("PreferredNodeID", func(t *testing.T) {
		t.Run("AlwaysSatisfiedWhenIdleAvailable", func(t *testing.T) {
			var createdCount atomic.Int32

			p := mustNewPool[*testItem, testItem](t,
				WithLimit[*testItem, testItem](3),
				WithCreateItemFunc(func(ctx context.Context) (*testItem, error) {
					idx := createdCount.Add(1) - 1

					return &testItem{
						v: idx,
						onNodeID: func() uint32 {
							nodeID, has := endpoint.ContextNodeID(ctx)
							if !has {
								nodeID = 0
							}

							return nodeID
						},
					}, nil
				}),
			)
			defer mustClose(t, p)
			requirePoolStats(t, p, poolStats(3, nil))

			// Fill the pool with 3 infos (nodeIDs: 0, 1, 2)
			info0 := mustGetItem(t, p)
			info1 := mustGetItem(t, p)
			_ = mustGetItem(t, p)
			requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3 }))

			// Put back infos 0 and 1 to make them idle
			mustPutItem(t, p, info0)
			mustPutItem(t, p, info1)
			requirePoolStats(t, p, poolStats(3, func(s *Stats) { s.Size = 3; s.Idle = 2 }))

			info1, _ = getItemWithFlush(endpoint.WithNodeID(t.Context(), 1), p)
			info2, _ := getItemWithFlush(endpoint.WithNodeID(t.Context(), 1), p)
			assert.Equal(t, uint32(1), info1.item.NodeID())
			assert.Equal(t, uint32(1), info2.item.NodeID())
			stats := p.Stats()
			assert.Equal(t, 3, stats.Limit)
			assert.LessOrEqual(t, stats.Idle, 3)
			assert.LessOrEqual(t, stats.Size, stats.Limit+2)
		})

		t.Run("RemovesIdleToMakeSpace", func(t *testing.T) {
			p := mustNewPool[*testItem, testItem](t,
				WithLimit[*testItem, testItem](2),
				WithCreateItemFunc(func(ctx context.Context) (*testItem, error) {
					nodeID, has := endpoint.ContextNodeID(ctx)
					if !has {
						nodeID = 0
					}

					return &testItem{
						onNodeID: func() uint32 {
							return nodeID
						},
					}, nil
				}),
			)
			defer mustClose(t, p)
			requirePoolStats(t, p, poolStats(2, nil))

			// Fill pool: 2 infos with nodeIDs 0 and 1
			info0 := mustGetItem(t, p)
			_ = mustGetItem(t, p)
			requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 2 }))

			// Put back info0 (now idle)
			mustPutItem(t, p, info0)
			requirePoolStats(t, p, poolStats(2, func(s *Stats) { s.Size = 2; s.Idle = 1 }))

			// Pool state: len=2, idle=1 (info0 with nodeID=0), busy=1 (info1)
			// Try to get info with preferred nodeID=99 (non-existent)
			// This should remove idle info and create new info with preferred nodeID
			getCtx := endpoint.WithNodeID(t.Context(), 99)
			info99, _ := getItemWithFlush(getCtx, p)

			// Item should have nodeID 99 (newly created with preferred)
			assert.Equal(t, uint32(99), info99.item.NodeID())
			stats := p.Stats()
			assert.Equal(t, 2, stats.Limit)
			assert.LessOrEqual(t, stats.Idle, 2)
			assert.LessOrEqual(t, stats.Size, stats.Limit+1)
		})

		t.Run("PreferredNodeIDAllBusy", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var idx atomic.Uint32
				p := mustNewPool[*testItem, testItem](t,
					WithLimit[*testItem, testItem](2),
					WithCreateItemFunc(func(ctx context.Context) (*testItem, error) {
						nodeID, has := endpoint.ContextNodeID(ctx)
						if !has {
							nodeID = idx.Add(1) - 1
						} else {
							idx.Add(1)
						}

						return &testItem{
							onNodeID: func() uint32 {
								return nodeID
							},
						}, nil
					}),
				)
				requirePoolStats(t, p, poolStats(2, nil))

				wait := make(chan struct{})
				holding := make(chan struct{}, 2)

				var finished sync.WaitGroup
				finished.Add(2)
				for range 2 {
					go func() {
						defer finished.Done()
						_ = p.With(t.Context(), func(ctx context.Context, item *testItem) error {
							holding <- struct{}{} // semaphore slot is taken
							<-wait

							return nil
						})
					}()
				}
				<-holding
				<-holding

				ctx := endpoint.WithNodeID(t.Context(), 3)
				getCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				var overflowItem bool
				err := p.With(getCtx, func(ctx context.Context, item *testItem) error {
					overflowItem = true

					return nil
				})
				assert.ErrorIs(t, err, context.DeadlineExceeded)
				assert.False(t, overflowItem)
				close(wait)
				finished.Wait()
				mustClose(t, p)
			})
		})
	})
}
