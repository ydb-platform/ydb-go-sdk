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
	"github.com/stretchr/testify/require"
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
		attempts int,
		nodeHintInfo *trace.NodeHintInfo,
		err error,
	) {
		return func(info any, attempts int, nodeHintInfo *trace.NodeHintInfo, err error) {
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

func mustGetItem[PT ItemConstraint[T], T any](t testing.TB, p *Pool[PT, T]) *itemInfo[PT, T] {
	info, err := p.getItem(t.Context())
	if err != nil {
		panic(err)
	}

	return info
}

func mustPutItem[PT ItemConstraint[T], T any](t testing.TB, p *Pool[PT, T], info *itemInfo[PT, T]) {
	if err := p.putItem(t.Context(), info); err != nil {
		panic(err)
	}
}

func mustClose(t testing.TB, pool closer.Closer) {
	if err := pool.Close(t.Context()); err != nil {
		panic(err)
	}
}

func TestPool(t *testing.T) { //nolint:gocyclo
	t.Run("New", func(t *testing.T) {
		t.Run("Default", func(t *testing.T) {
			p := New[*testItem, testItem](t.Context(),
				WithTrace[*testItem, testItem](defaultTrace),
			)
			err := p.With(t.Context(), func(ctx context.Context, testItem *testItem) error {
				require.EqualValues(t, 0, testItem.NodeID())

				return nil
			})
			require.NoError(t, err)
		})
		t.Run("RequireNodeIdFromPool", func(t *testing.T) {
			hintTrace := defaultTrace
			var preferredID uint32
			hintTrace.OnGet = func(ctx *context.Context, call stack.Caller) func(
				info any,
				attempts int,
				nodeHintInfo *trace.NodeHintInfo,
				err error,
			) {
				return func(info any, attempts int, nodeHintInfo *trace.NodeHintInfo, err error) {
					if nodeHintInfo != nil {
						preferredID = nodeHintInfo.PreferredNodeID
					}
				}
			}
			var newItemCalled uint32
			p := New[*testItem, testItem](t.Context(),
				WithTrace[*testItem, testItem](defaultTrace),
				WithCreateItemFunc(func(ctx context.Context) (*testItem, error) {
					newItemCalled++
					v := testItem{
						v: 0,
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

			info := mustGetItem(t, p)
			require.EqualValues(t, 0, info.item.NodeID())
			require.EqualValues(t, true, info.item.IsAlive())
			mustPutItem(t, p, info)

			info, err := p.getItem(endpoint.WithNodeID(t.Context(), 32))
			require.NoError(t, err)
			require.EqualValues(t, 32, info.item.NodeID())
			mustPutItem(t, p, info)

			info, err = p.getItem(endpoint.WithNodeID(t.Context(), 33))
			require.NoError(t, err)
			require.EqualValues(t, 33, info.item.NodeID())
			mustPutItem(t, p, info)

			info, err = p.getItem(endpoint.WithNodeID(t.Context(), 32))
			require.NoError(t, err)
			require.EqualValues(t, 32, info.item.NodeID())
			mustPutItem(t, p, info)

			info, err = p.getItem(endpoint.WithNodeID(t.Context(), 33))
			require.NoError(t, err)
			require.EqualValues(t, 33, info.item.NodeID())
			mustPutItem(t, p, info)

			info, err = p.getItem(endpoint.WithNodeID(t.Context(), 32))
			require.NoError(t, err)
			info2, err := p.getItem(endpoint.WithNodeID(t.Context(), 33))
			require.NoError(t, err)
			require.EqualValues(t, 32, info.item.NodeID())
			require.EqualValues(t, 33, info2.item.NodeID())
			mustPutItem(t, p, info2)
			mustPutItem(t, p, info)

			info, err = p.getItem(endpoint.WithNodeID(t.Context(), 32))
			require.NoError(t, err)
			info2, err = p.getItem(endpoint.WithNodeID(t.Context(), 33))
			require.NoError(t, err)
			require.EqualValues(t, 32, info.item.NodeID())
			require.EqualValues(t, 33, info2.item.NodeID())
			mustPutItem(t, p, info)
			mustPutItem(t, p, info2)

			info, err = p.getItem(endpoint.WithNodeID(t.Context(), 32))
			require.NoError(t, err)
			info2, err = p.getItem(endpoint.WithNodeID(t.Context(), 33))
			require.NoError(t, err)
			info3, err := p.getItem(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 32, info.item.NodeID())
			require.EqualValues(t, 33, info2.item.NodeID())
			require.EqualValues(t, 0, info3.item.NodeID())
			mustPutItem(t, p, info)
			mustPutItem(t, p, info2)
			mustPutItem(t, p, info3)
			info, err = p.getItem(endpoint.WithNodeID(t.Context(), 100))
			require.NoError(t, err)
			require.EqualValues(t, 100, preferredID)
			require.EqualValues(t, 100, info.item.NodeID())
			require.EqualValues(t, 4, newItemCalled)
			_, _ = p.getItem(endpoint.WithNodeID(t.Context(), 32))
			_, _ = p.getItem(endpoint.WithNodeID(t.Context(), 32))
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			// should not panic
			_, _ = p.getItem(endpoint.WithNodeID(ctx, 32))
		})
		t.Run("PreferredNodeTakenAndPoolFull", func(t *testing.T) {
			p := New[*testItem, testItem](t.Context(),
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

			info1, err := p.getItem(endpoint.WithNodeID(t.Context(), 32))
			require.NoError(t, err)
			require.EqualValues(t, 32, info1.item.NodeID())
			info2, err := p.getItem(endpoint.WithNodeID(t.Context(), 33))
			require.NoError(t, err)
			require.EqualValues(t, 33, info2.item.NodeID())

			mustPutItem(t, p, info2)

			info3, err := p.getItem(endpoint.WithNodeID(t.Context(), 32))
			require.NoError(t, err)
			require.EqualValues(t, 32, info3.item.NodeID())
		})
		t.Run("CreateItemOnGivenNode", func(t *testing.T) {
			var newItemCalled uint32
			p := New[*testItem, testItem](t.Context(),
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

			info, err := p.getItem(endpoint.WithNodeID(t.Context(), 32))
			require.NoError(t, err)
			require.NotNil(t, info)
			require.NotNil(t, info.item)
			require.EqualValues(t, 32, info.item.NodeID())
			require.EqualValues(t, true, info.item.IsAlive())
			mustPutItem(t, p, info)

			info = mustGetItem(t, p)
			require.EqualValues(t, 32, info.item.NodeID())
			mustPutItem(t, p, info)

			require.EqualValues(t, 1, newItemCalled)
		})
		t.Run("WithLimit", func(t *testing.T) {
			p := New[*testItem, testItem](t.Context(), WithLimit[*testItem, testItem](1),
				WithTrace[*testItem, testItem](defaultTrace),
			)
			require.EqualValues(t, 1, p.config.limit)
		})
		t.Run("WithItemUsageLimit", func(t *testing.T) {
			var (
				newCounter        int64
				errMustDeleteItem = xerrors.Retryable(errors.New("test"))
			)
			p := New[*testItem, testItem](t.Context(),
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
			require.EqualValues(t, 1, p.config.limit)
			var lambdaCounter atomic.Int64
			err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
				if lambdaCounter.Add(1) < 10 {
					return xerrors.WithStackTrace(errMustDeleteItem)
				}

				return nil
			})
			require.NoError(t, err)
			require.EqualValues(t, 10, newCounter)
		})
		t.Run("WithCreateItemFunc", func(t *testing.T) {
			var newCounter atomic.Int64
			p := New(t.Context(),
				WithLimit[*testItem, testItem](1),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					newCounter.Add(1)
					var v testItem

					return &v, nil
				}),
				WithTrace[*testItem, testItem](defaultTrace),
			)
			err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
				return nil
			})
			require.NoError(t, err)
			require.EqualValues(t, p.config.limit, newCounter.Load())
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

			p := New[*testItem, testItem](t.Context(),
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

			defer func() {
				_ = p.Close(t.Context())
			}()

			require.Zero(t, p.idle.Len())

			var (
				s1 = mustGetItem(t, p)
				s2 = mustGetItem(t, p)
				s3 = mustGetItem(t, p)
			)

			require.Zero(t, p.idle.Len())

			mustPutItem(t, p, s1)
			mustPutItem(t, p, s2)

			require.Equal(t, 2, p.idle.Len())

			mustClose(t, p)

			require.Zero(t, p.idle.Len())

			require.True(t, closed[0])  // idle info in pool
			require.True(t, closed[1])  // idle info in pool
			require.False(t, closed[2]) // info extracted from idle but closed later on putItem

			require.ErrorIs(t, p.putItem(t.Context(), s3), errClosedPool)

			require.True(t, closed[2]) // after putItem s3 must be closed
		})
		t.Run("WithCancelledContext", func(t *testing.T) {
			// Regression test: closing idle infos from the pool must succeed even
			// when the context passed to Close is already cancelled.
			xtest.TestManyTimes(t, func(t testing.TB) {
				var closedCount atomic.Int32

				p := New[*testItem, testItem](t.Context(),
					WithLimit[*testItem, testItem](3),
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						var v testItem

						return &v, nil
					}),
					WithTrace[*testItem, testItem](defaultTrace),
				)

				// Override the close func to detect context-cancelled failures.
				// In a real scenario (e.g. gRPC session close), a cancelled context
				// would cause the close call to fail immediately.
				p.config.closeItemFunc = func(ctx context.Context, info *testItem) {
					closedCount.Add(1)
				}

				info1 := mustGetItem(t, p)
				info2 := mustGetItem(t, p)
				info3 := mustGetItem(t, p)

				mustPutItem(t, p, info1)
				mustPutItem(t, p, info2)

				require.Equal(t, 2, p.idle.Len())

				// Close the pool with an already-cancelled context.
				cancelCtx, cancel := context.WithCancel(t.Context())
				cancel()

				err := p.Close(cancelCtx)
				require.NoError(t, err)

				// Both idle infos must have been closed despite the cancelled context.
				require.Equal(t, int32(2), closedCount.Load())

				_ = info3 // info3 is still "in use" and not in the idle list
			})
		})
		//t.Run("WhenWaiting", func(t *testing.T) {
		//	for _, test := range []struct {
		//		name string
		//		racy bool
		//	}{
		//		{
		//			name: "normal",
		//			racy: false,
		//		},
		//		{
		//			name: "racy",
		//			racy: true,
		//		},
		//	} {
		//		t.Run(test.name, func(t *testing.T) {
		//			var (
		//				get  = make(chan struct{})
		//				wait = make(chan struct{})
		//				got  = make(chan error)
		//			)
		//			p := New[*testItem, testItem](t.Context(),
		//				WithLimit[*testItem, testItem](1),
		//				WithTrace[*testItem, testItem](&Trace{
		//					onWait: func() func(info any, err error) {
		//						wait <- struct{}{}
		//
		//						return nil
		//					},
		//				}),
		//			)
		//			defer func() {
		//				_ = p.Close(t.Context())
		//			}()
		//
		//			// first call getItem creates an info and store in index
		//			// second call getItem from pool with limit === 1 will skip
		//			// create info step (because pool have not enough space for
		//			// creating new infos) and will freeze until wait free info from pool
		//			mustGetItem(t, p)
		//
		//			go func() {
		//				p.config.trace.OnGet = func(ctx *context.Context, call stack.Caller) func(
		//					info any,
		//					attempts int,
		//					_ *trace.NodeHintInfo,
		//					err error,
		//				) {
		//					get <- struct{}{}
		//
		//					return nil
		//				}
		//
		//				_, err := p.getItem(t.Context())
		//				got <- err
		//			}()
		//
		//			<-get // Await for getter blocked on awaiting info.
		//
		//			if test.racy {
		//				// We are testing the case, when info consumer registered
		//				// himself in the wait queue, but not ready to receive the
		//				// info when info arrives (that is, stuck between
		//				// pushing channel in the list and reading from the channel).
		//				_ = p.Close(t.Context())
		//				<-wait
		//			} else {
		//				// We are testing the normal case, when info consumer registered
		//				// himself in the wait queue and successfully blocked on
		//				// reading from signaling channel.
		//				<-wait
		//				// Let the waiting goroutine to block on reading from channel.
		//				_ = p.Close(t.Context())
		//			}
		//
		//			const timeout = time.Second
		//			select {
		//			case err := <-got:
		//				if !xerrors.Is(err, errClosedPool) {
		//					t.Fatalf(
		//						"unexpected error: %q; want %q'",
		//						err, errClosedPool,
		//					)
		//				}
		//			case <-p.config.clock.After(timeout):
		//				t.Fatalf("no result after %s", timeout)
		//			}
		//		})
		//	}
		//})
		t.Run("IdleItems", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					created       atomic.Int32
					idleThreshold = 4 * time.Second
					closedCount   atomic.Int64
					fakeClock     = clockwork.NewFakeClock()
				)
				p := New[*testItem, testItem](t.Context(),
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

				info1 := mustGetItem(t, p)
				info2 := mustGetItem(t, p)

				// Put both infos at the absolutely same time.
				// That is, both infos must be updated their lastUsage timestamp.
				mustPutItem(t, p, info1)
				mustPutItem(t, p, info2)

				require.Equal(t, 2, p.idle.Len())

				// Move clock to longer than idleTimeToLive
				fakeClock.Advance(idleThreshold + time.Nanosecond)

				// on get info from idle list the pool must check the info idle timestamp
				// both existing infos must be closed
				// getItem must create a new info and return it from getItem
				info3 := mustGetItem(t, p)

				if !closedCount.CompareAndSwap(2, 0) {
					t.Fatal("unexpected number of closed info's")
				}

				// Move time to idleTimeToLive / 2 - this emulate a "spent" some time working within info.
				fakeClock.Advance(idleThreshold / 2)

				// Now put that info back
				// pool must update a lastUsage timestamp of info
				mustPutItem(t, p, info3)

				// Move time to idleTimeToLive / 2
				// Total time since last updating lastUsage timestampe is more than idleTimeToLive
				fakeClock.Advance(idleThreshold/2 + time.Nanosecond)

				require.Equal(t, 1, p.idle.Len())

				info4 := mustGetItem(t, p)
				require.Equal(t, 0, p.idle.Len())
				mustPutItem(t, p, info4)

				_ = p.Close(t.Context())

				require.Equal(t, 0, p.idle.Len())
			}, xtest.StopAfter(3*time.Second))
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
						name: "Cnacelled",
						err:  context.Canceled,
					},
					{
						name: "DeadlineExceeded",
						err:  context.DeadlineExceeded,
					},
				} {
					t.Run(tt.name, func(t *testing.T) {
						var counter atomic.Int64
						p := New(t.Context(),
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
						err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
							return nil
						})
						require.NoError(t, err)
						require.GreaterOrEqual(t, counter.Load(), int64(10))
					})
				}
			})
			t.Run("OnTransportError", func(t *testing.T) {
				var counter atomic.Int64
				p := New(t.Context(),
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
				err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
					return nil
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, counter.Load(), int64(10))
			})
			t.Run("OnOperationError", func(t *testing.T) {
				var counter atomic.Int64
				p := New(t.Context(),
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
				err := p.With(t.Context(), func(ctx context.Context, info *testItem) error {
					return nil
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, counter.Load(), int64(10))
			})
			t.Run("NilNil", func(t *testing.T) {
				xtest.TestManyTimes(t, func(t testing.TB) {
					limit := 100
					ctx, cancel := xcontext.WithTimeout(
						t.Context(),
						55*time.Second,
					)
					defer cancel()
					p := New[*testItem, testItem](t.Context())
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
						s, err := p.createItemFunc(childCtx)
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
					p := New[*testItem, testItem](ctx, WithLimit[*testItem, testItem](1))
					err := p.With(ctx, func(ctx context.Context, testItem *testItem) error {
						return nil
					})
					require.ErrorIs(t, err, context.Canceled)
				})
				t.Run("DeadlineExceeded", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 0)
					cancel()
					p := New[*testItem, testItem](ctx, WithLimit[*testItem, testItem](1))
					err := p.With(ctx, func(ctx context.Context, testItem *testItem) error {
						return nil
					})
					require.ErrorIs(t, err, context.DeadlineExceeded)
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
				t.Run("", func(t *testing.T) {
					backoff := make(chan chan time.Time)
					ctx, cancel := xcontext.WithCancel(t.Context())
					p := New[*testItem, testItem](ctx, WithLimit[*testItem, testItem](1))

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
				p := New(t.Context(),
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
				err := p.With(t.Context(), func(ctx context.Context, testItem *testItem) error {
					return nil
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, createCounter.Load(), closeCounter.Load())
				err = p.Close(t.Context())
				require.NoError(t, err)
				require.EqualValues(t, createCounter.Load(), closeCounter.Load())
			})
		})
		t.Run("IsAlive", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					newItems    atomic.Int64
					deleteItems atomic.Int64
					expErr      = errors.New("expected error")
				)
				p := New(t.Context(),
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
				err := p.With(t.Context(), func(ctx context.Context, testItem *testItem) error {
					if newItems.Load() < 10 {
						return xerrors.Retryable(expErr, xerrors.Invalid(testItem))
					}

					return nil
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, newItems.Load(), int64(9))
				require.GreaterOrEqual(t, newItems.Load(), deleteItems.Load())
				err = p.Close(t.Context())
				require.NoError(t, err)
				require.EqualValues(t, newItems.Load(), deleteItems.Load())
			}, xtest.StopAfter(3*time.Second))
		})
	})
	t.Run("With", func(t *testing.T) {
		t.Run("SpoiledIdleItems", func(t *testing.T) {
			// Models query sessions that become !IsAlive() while sitting in idle
			// (e.g. attach stream Recv error in listenAttachStream).
			ctx := t.Context()

			var (
				created atomic.Int32
				closed  atomic.Int32
				spoiled sync.Map // item id -> struct{}
			)

			p := New[*testItem, testItem](ctx,
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

			err := p.With(ctx, func(context.Context, *testItem) error {
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, int32(1), created.Load())
			require.Equal(t, int32(0), closed.Load())
			require.Equal(t, 1, p.Stats().Idle)

			spoiled.Store(int32(1), struct{}{})

			var gotID int32
			err = p.With(ctx, func(_ context.Context, item *testItem) error {
				gotID = item.v

				return nil
			})
			require.NoError(t, err)
			require.Equal(t, int32(2), gotID, "must not reuse spoiled idle item")
			require.Equal(t, int32(2), created.Load())
			require.Equal(t, int32(1), closed.Load(), "spoiled idle item must be closed on get")
			require.Equal(t, 1, p.Stats().Idle)

			spoiled.Store(int32(2), struct{}{})

			err = p.With(ctx, func(_ context.Context, item *testItem) error {
				gotID = item.v

				return nil
			})
			require.NoError(t, err)
			require.Equal(t, int32(3), gotID)
			require.Equal(t, int32(3), created.Load())
			require.Equal(t, int32(2), closed.Load())
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
			p := New[*testItem, testItem](t.Context(),
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

			info1 := mustGetItem(t, p)
			assertClosed(0)
			assertCreated(1)

			mustPutItem(t, p, info1)
			assertClosed(0)
			assertCreated(1)
			require.Equal(t, 1, p.idle.Len())

			info2, err := p.getItem(t.Context())
			require.NoError(t, err)
			assertCreated(1)
			assertClosed(0)
			require.Equal(t, 0, p.idle.Len())

			_, err = p.getItem(t.Context())
			require.NoError(t, err)
			assertCreated(2)
			assertClosed(0)
			require.Equal(t, 0, p.idle.Len())

			require.NoError(t, p.Close(t.Context()))
			assertCreated(2)
			assertClosed(0)

			require.Equal(t, 0, p.idle.Len())

			require.ErrorIs(t, p.putItem(t.Context(), info2), errClosedPool)
			assertClosed(1)

			require.True(t, info2.item.closed)
			require.False(t, info2.item.IsAlive())

			require.Equal(t, 0, p.idle.Len())
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
			p := New[*testItem, testItem](t.Context(),
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

			info := mustGetItem(t, p)
			assertCreated(1)

			mustPutItem(t, p, info)
			assertClosed(0)

			mustGetItem(t, p)
			assertCreated(1)

			info.item.Close(t.Context())

			assertClosed(1)

			mustGetItem(t, p)
			assertCreated(2)
		})
		t.Run("OnErrorWithDeleteItem", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					created           atomic.Int32
					errMustDeleteItem = errors.New("info must be deleted")
				)
				p := New[*testItem, testItem](t.Context(),
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

				errThrown := false
				err := p.With(t.Context(), func(ctx context.Context, testItem *testItem) error {
					if errThrown {
						require.EqualValues(t, 2, testItem.v)

						return nil
					}

					require.EqualValues(t, 1, testItem.v)

					defer func() {
						errThrown = true
					}()

					return xerrors.Retryable(errMustDeleteItem)
				})

				require.NoError(t, err)
			}, xtest.StopAfter(5*time.Second))
		})
		//t.Run("Racy", func(t *testing.T) {
		//	xtest.TestManyTimes(t, func(t testing.TB) {
		//		trace := &Trace{
		//			OnChange: func(stats Stats) {
		//				require.GreaterOrEqual(t, stats.Limit, stats.Idle)
		//			},
		//		}
		//		p := New[*testItem, testItem](t.Context(),
		//			WithTrace[*testItem, testItem](trace),
		//		)
		//		r := xrand.New(xrand.WithLock())
		//		var wg sync.WaitGroup
		//		wg.Add(DefaultLimit*2 + 1)
		//		for range make([]struct{}, DefaultLimit*2) {
		//			go func() {
		//				defer wg.Done()
		//				childCtx, childCancel := xcontext.WithTimeout(
		//					t.Context(),
		//					time.Duration(r.Int64(int64(time.Second))),
		//				)
		//				defer childCancel()
		//				err := p.With(childCtx, func(ctx context.Context, testItem *testItem) error {
		//					return nil
		//				})
		//				if err != nil && !xerrors.Is(err, errClosedPool, context.Canceled) {
		//					t.Failed()
		//				}
		//			}()
		//		}
		//		go func() {
		//			defer wg.Done()
		//			time.Sleep(time.Millisecond)
		//			err := p.Close(t.Context())
		//			require.NoError(t, err)
		//		}()
		//		wg.Wait()
		//	})
		//})
		t.Run("ParallelCreation", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				trace := &Trace{
					OnChange: func(stats Stats) {
						require.Equal(t, DefaultLimit, stats.Limit)
						require.LessOrEqual(t, stats.Idle, DefaultLimit)
					},
				}
				p := New[*testItem, testItem](t.Context(),
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithTrace[*testItem, testItem](trace),
				)
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
						require.LessOrEqual(t, stats.Idle, DefaultLimit)
					}()
				}

				wg.Wait()
			})
		})
		//t.Run("PutTwice", func(t *testing.T) {
		//	p := New(t.Context(),
		//		WithLimit[*testItem, testItem](2),
		//		WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
		//		WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
		//	)
		//	info := mustGetItem(t, p)
		//
		//	mustPutItem(t, p, info)
		//
		//	require.Panics(t, func() {
		//		mustPutItem(t, p, info)
		//	})
		//})
	})
	t.Run("PreferredNodeID", func(t *testing.T) {
		t.Run("AlwaysSatisfiedWhenIdleAvailable", func(t *testing.T) {
			var createdCount atomic.Int32

			p := New[*testItem, testItem](t.Context(),
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

			// Fill the pool with 3 infos (nodeIDs: 0, 1, 2)
			info0 := mustGetItem(t, p)
			info1 := mustGetItem(t, p)
			_ = mustGetItem(t, p)

			// Put back infos 0 and 1 to make them idle
			mustPutItem(t, p, info0)
			mustPutItem(t, p, info1)

			info1, _ = p.getItem(endpoint.WithNodeID(t.Context(), 1))
			info2, _ := p.getItem(endpoint.WithNodeID(t.Context(), 1))
			require.Equal(t, uint32(1), info1.item.NodeID())
			require.Equal(t, uint32(1), info2.item.NodeID())

			// Pool should still be within limit
			stats := p.Stats()
			require.Equal(t, 3, stats.Limit)
			require.LessOrEqual(t, stats.Index, 3)
		})

		t.Run("RemovesIdleToMakeSpace", func(t *testing.T) {
			p := New[*testItem, testItem](t.Context(),
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

			// Fill pool: 2 infos with nodeIDs 0 and 1
			info0 := mustGetItem(t, p)
			_ = mustGetItem(t, p)

			// Put back info0 (now idle)
			mustPutItem(t, p, info0)

			// Pool state: len=2, idle=1 (info0 with nodeID=0), busy=1 (info1)
			// Try to get info with preferred nodeID=99 (non-existent)
			// This should remove idle info and create new info with preferred nodeID
			getCtx := endpoint.WithNodeID(t.Context(), 99)
			info99, _ := p.getItem(getCtx)

			// Item should have nodeID 99 (newly created with preferred)
			require.Equal(t, uint32(99), info99.item.NodeID())

			// Pool should not exceed limit
			stats := p.Stats()
			require.LessOrEqual(t, stats.Index, 2)
		})

		t.Run("PreferredNodeIDAllBusy", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var idx uint32
				p := New[*testItem, testItem](t.Context(),
					WithLimit[*testItem, testItem](2),
					WithCreateItemFunc(func(ctx context.Context) (*testItem, error) {
						nodeID, has := endpoint.ContextNodeID(ctx)
						if !has {
							nodeID = idx
						}
						idx = idx + 1

						return &testItem{
							onNodeID: func() uint32 {
								return nodeID
							},
						}, nil
					}),
				)

				wait := make(chan struct{})

				var (
					started  sync.WaitGroup
					finished sync.WaitGroup
				)
				started.Add(2)
				finished.Add(2)
				for range 2 {
					go func() {
						started.Done()
						_ = p.With(t.Context(), func(ctx context.Context, item *testItem) error {
							defer finished.Done()

							<-wait

							return nil
						})
					}()
				}
				started.Wait()

				ctx := endpoint.WithNodeID(t.Context(), 3)
				getCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				var overflowItem bool
				err := p.With(getCtx, func(ctx context.Context, item *testItem) error {
					overflowItem = true

					return nil
				})
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.False(t, overflowItem)
				close(wait)
				finished.Wait()
				mustClose(t, p)
			})
		})
	})
}
