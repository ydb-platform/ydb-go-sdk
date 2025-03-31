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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

type (
	testItem struct {
		v int32

		closed bool

		onClose   func() error
		onIsAlive func() bool
		onNodeID  func() uint32
	}
	testWaitChPool struct {
		xsync.Pool[chan *testItem]
		testHookGetWaitCh func()
	}
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
	OnPut: func(ctx *context.Context, call stack.Caller, item any) func(err error) {
		return func(err error) {
		}
	},
	OnGet: func(ctx *context.Context, call stack.Caller) func(item any, attempts int, err error) {
		return func(item any, attempts int, err error) {
		}
	},
	onWait: func() func(item any, err error) {
		return func(item any, err error) {
		}
	},
	OnChange: func(stats Stats) {
	},
}

func (p *testWaitChPool) GetOrNew() *chan *testItem {
	if p.testHookGetWaitCh != nil {
		p.testHookGetWaitCh()
	}

	return p.Pool.GetOrNew()
}

func (p *testWaitChPool) whenWantWaitCh() <-chan struct{} {
	var (
		prev = p.testHookGetWaitCh
		ch   = make(chan struct{})
	)
	p.testHookGetWaitCh = func() {
		p.testHookGetWaitCh = prev
		close(ch)
	}

	return ch
}

func (p *testWaitChPool) Put(ch *chan *testItem) {}

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

func mustGetItem[PT ItemConstraint[T], T any](t testing.TB, p *Pool[PT, T]) PT {
	s, err := p.getItem(context.Background())
	if err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}

	return s
}

func mustPutItem[PT ItemConstraint[T], T any](t testing.TB, p *Pool[PT, T], item PT) {
	if err := p.putItem(context.Background(), item); err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}
}

func mustClose(t testing.TB, pool closer.Closer) {
	if err := pool.Close(context.Background()); err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}
}

func TestPool(t *testing.T) { //nolint:gocyclo
	rootCtx := xtest.Context(t)
	t.Run("New", func(t *testing.T) {
		t.Run("Default", func(t *testing.T) {
			p := New[*testItem, testItem](rootCtx,
				WithTrace[*testItem, testItem](defaultTrace),
			)
			err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
				require.EqualValues(t, 0, testItem.NodeID())

				return nil
			})
			require.NoError(t, err)
		})
		t.Run("RequireNodeIdFromPool", func(t *testing.T) {
			nextNodeID := uint32(0)
			var newItemCalled uint32
			p := New[*testItem, testItem](rootCtx,
				WithTrace[*testItem, testItem](defaultTrace),
				WithCreateItemFunc(func(ctx context.Context) (*testItem, error) {
					newItemCalled++
					var (
						nodeID = nextNodeID
						v      = testItem{
							v: 0,
							onNodeID: func() uint32 {
								return nodeID
							},
						}
					)

					return &v, nil
				}),
			)

			item := mustGetItem(t, p)
			require.EqualValues(t, 0, item.NodeID())
			require.EqualValues(t, true, item.IsAlive())
			mustPutItem(t, p, item)

			nextNodeID = 32

			item, err := p.getItem(endpoint.WithNodeID(context.Background(), 32))
			require.NoError(t, err)
			require.EqualValues(t, 32, item.NodeID())
			mustPutItem(t, p, item)

			nextNodeID = 33

			item, err = p.getItem(endpoint.WithNodeID(context.Background(), 33))
			require.NoError(t, err)
			require.EqualValues(t, 33, item.NodeID())
			mustPutItem(t, p, item)

			item, err = p.getItem(endpoint.WithNodeID(context.Background(), 32))
			require.NoError(t, err)
			require.EqualValues(t, 32, item.NodeID())
			mustPutItem(t, p, item)

			item, err = p.getItem(endpoint.WithNodeID(context.Background(), 33))
			require.NoError(t, err)
			require.EqualValues(t, 33, item.NodeID())
			mustPutItem(t, p, item)

			item, err = p.getItem(endpoint.WithNodeID(context.Background(), 32))
			require.NoError(t, err)
			item2, err := p.getItem(endpoint.WithNodeID(context.Background(), 33))
			require.NoError(t, err)
			require.EqualValues(t, 32, item.NodeID())
			require.EqualValues(t, 33, item2.NodeID())
			mustPutItem(t, p, item2)
			mustPutItem(t, p, item)

			item, err = p.getItem(endpoint.WithNodeID(context.Background(), 32))
			require.NoError(t, err)
			item2, err = p.getItem(endpoint.WithNodeID(context.Background(), 33))
			require.NoError(t, err)
			require.EqualValues(t, 32, item.NodeID())
			require.EqualValues(t, 33, item2.NodeID())
			mustPutItem(t, p, item)
			mustPutItem(t, p, item2)

			item, err = p.getItem(endpoint.WithNodeID(context.Background(), 32))
			require.NoError(t, err)
			item2, err = p.getItem(endpoint.WithNodeID(context.Background(), 33))
			require.NoError(t, err)
			item3, err := p.getItem(context.Background())
			require.NoError(t, err)
			require.EqualValues(t, 32, item.NodeID())
			require.EqualValues(t, 33, item2.NodeID())
			require.EqualValues(t, 0, item3.NodeID())
			mustPutItem(t, p, item)
			mustPutItem(t, p, item2)
			mustPutItem(t, p, item3)

			require.EqualValues(t, 3, newItemCalled)
		})
		t.Run("CreateItemOnGivenNode", func(t *testing.T) {
			var newItemCalled uint32
			p := New[*testItem, testItem](rootCtx,
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

			item, err := p.getItem(endpoint.WithNodeID(context.Background(), 32))
			require.NoError(t, err)
			require.EqualValues(t, 32, item.NodeID())
			require.EqualValues(t, true, item.IsAlive())
			mustPutItem(t, p, item)

			item = mustGetItem(t, p)
			require.EqualValues(t, 32, item.NodeID())
			mustPutItem(t, p, item)

			require.EqualValues(t, 1, newItemCalled)
		})
		t.Run("WithLimit", func(t *testing.T) {
			p := New[*testItem, testItem](rootCtx, WithLimit[*testItem, testItem](1),
				WithTrace[*testItem, testItem](defaultTrace),
			)
			require.EqualValues(t, 1, p.config.limit)
		})
		t.Run("WithItemUsageLimit", func(t *testing.T) {
			var newCounter int64
			p := New[*testItem, testItem](rootCtx,
				WithLimit[*testItem, testItem](1),
				WithItemUsageLimit[*testItem, testItem](5),
				WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					atomic.AddInt64(&newCounter, 1)

					var v testItem

					return &v, nil
				}),
			)
			require.EqualValues(t, 1, p.config.limit)
			var lambdaCounter int64
			err := p.With(rootCtx, func(ctx context.Context, item *testItem) error {
				if atomic.AddInt64(&lambdaCounter, 1) < 10 {
					return xerrors.Retryable(errors.New("test"))
				}

				return nil
			})
			require.NoError(t, err)
			require.EqualValues(t, 2, newCounter)
		})
		t.Run("WithCreateItemFunc", func(t *testing.T) {
			var newCounter int64
			p := New(rootCtx,
				WithLimit[*testItem, testItem](1),
				WithCreateItemFunc(func(context.Context) (*testItem, error) {
					atomic.AddInt64(&newCounter, 1)
					var v testItem

					return &v, nil
				}),
				WithTrace[*testItem, testItem](defaultTrace),
			)
			err := p.With(rootCtx, func(ctx context.Context, item *testItem) error {
				return nil
			})
			require.NoError(t, err)
			require.EqualValues(t, p.config.limit, atomic.LoadInt64(&newCounter))
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

			p := New[*testItem, testItem](rootCtx,
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
				_ = p.Close(context.Background())
			}()

			require.Empty(t, p.index)
			require.Zero(t, p.idle.Len())

			var (
				s1 = mustGetItem(t, p)
				s2 = mustGetItem(t, p)
				s3 = mustGetItem(t, p)
			)

			require.Len(t, p.index, 3)
			require.Zero(t, p.idle.Len())

			mustPutItem(t, p, s1)
			mustPutItem(t, p, s2)

			require.Len(t, p.index, 3)
			require.Equal(t, 2, p.idle.Len())

			mustClose(t, p)

			require.Len(t, p.index, 1)
			require.Zero(t, p.idle.Len())

			require.True(t, closed[0])  // idle item in pool
			require.True(t, closed[1])  // idle item in pool
			require.False(t, closed[2]) // item extracted from idle but closed later on putItem

			require.ErrorIs(t, p.putItem(context.Background(), s3), errClosedPool)

			require.True(t, closed[2]) // after putItem s3 must be closed
		})
		t.Run("WhenWaiting", func(t *testing.T) {
			for _, test := range []struct {
				name string
				racy bool
			}{
				{
					name: "normal",
					racy: false,
				},
				{
					name: "racy",
					racy: true,
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					var (
						get  = make(chan struct{})
						wait = make(chan struct{})
						got  = make(chan error)
					)
					waitChPool := &testWaitChPool{
						Pool: xsync.Pool[chan *testItem]{
							New: func() *chan *testItem {
								ch := make(chan *testItem)

								return &ch
							},
						},
					}
					p := New[*testItem, testItem](rootCtx,
						WithLimit[*testItem, testItem](1),
						WithTrace[*testItem, testItem](&Trace{
							onWait: func() func(item any, err error) {
								wait <- struct{}{}

								return nil
							},
						}),
					)
					p.waitChPool = waitChPool
					defer func() {
						_ = p.Close(context.Background())
					}()

					// first call getItem creates an item and store in index
					// second call getItem from pool with limit === 1 will skip
					// create item step (because pool have not enough space for
					// creating new items) and will freeze until wait free item from pool
					mustGetItem(t, p)

					go func() {
						p.config.trace.OnGet = func(ctx *context.Context, call stack.Caller) func(item any, attempts int, err error) {
							get <- struct{}{}

							return nil
						}

						_, err := p.getItem(context.Background())
						got <- err
					}()

					regWait := waitChPool.whenWantWaitCh()
					<-get     // Await for getter blocked on awaiting item.
					<-regWait // Let the getter register itself in the wait queue.

					if test.racy {
						// We are testing the case, when item consumer registered
						// himself in the wait queue, but not ready to receive the
						// item when item arrives (that is, stuck between
						// pushing channel in the list and reading from the channel).
						_ = p.Close(context.Background())
						<-wait
					} else {
						// We are testing the normal case, when item consumer registered
						// himself in the wait queue and successfully blocked on
						// reading from signaling channel.
						<-wait
						// Let the waiting goroutine to block on reading from channel.
						_ = p.Close(context.Background())
					}

					const timeout = time.Second
					select {
					case err := <-got:
						if !xerrors.Is(err, errClosedPool) {
							t.Fatalf(
								"unexpected error: %q; want %q'",
								err, errClosedPool,
							)
						}
					case <-p.config.clock.After(timeout):
						t.Fatalf("no result after %s", timeout)
					}
				})
			}
		})
		t.Run("IdleItems", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					idleThreshold = 4 * time.Second
					closedCount   atomic.Int64
					fakeClock     = clockwork.NewFakeClock()
				)
				p := New[*testItem, testItem](rootCtx,
					WithLimit[*testItem, testItem](2),
					WithCreateItemTimeout[*testItem, testItem](0),
					WithCreateItemFunc[*testItem, testItem](func(ctx context.Context) (*testItem, error) {
						v := testItem{
							v: 0,
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

				s1 := mustGetItem(t, p)
				s2 := mustGetItem(t, p)

				// Put both items at the absolutely same time.
				// That is, both items must be updated their lastUsage timestamp.
				mustPutItem(t, p, s1)
				mustPutItem(t, p, s2)

				require.Len(t, p.index, 2)
				require.Equal(t, 2, p.idle.Len())

				// Move clock to longer than idleTimeToLive
				fakeClock.Advance(idleThreshold + time.Nanosecond)

				// on get item from idle list the pool must check the item idle timestamp
				// both existing items must be closed
				// getItem must create a new item and return it from getItem
				s3 := mustGetItem(t, p)

				require.Len(t, p.index, 1)

				if !closedCount.CompareAndSwap(2, 0) {
					t.Fatal("unexpected number of closed items")
				}

				// Move time to idleTimeToLive / 2 - this emulate a "spent" some time working within item.
				fakeClock.Advance(idleThreshold / 2)

				// Now put that item back
				// pool must update a lastUsage timestamp of item
				mustPutItem(t, p, s3)

				// Move time to idleTimeToLive / 2
				// Total time since last updating lastUsage timestampe is more than idleTimeToLive
				fakeClock.Advance(idleThreshold/2 + time.Nanosecond)

				require.Len(t, p.index, 1)
				require.Equal(t, 1, p.idle.Len())

				s4 := mustGetItem(t, p)
				require.Equal(t, s3, s4)
				require.Len(t, p.index, 1)
				require.Equal(t, 0, p.idle.Len())
				mustPutItem(t, p, s4)

				_ = p.Close(context.Background())

				require.Empty(t, p.index)
				require.Equal(t, 0, p.idle.Len())
			}, xtest.StopAfter(3*time.Second))
		})
	})
	t.Run("Retry", func(t *testing.T) {
		t.Run("CreateItem", func(t *testing.T) {
			t.Run("context", func(t *testing.T) {
				t.Run("Cancelled", func(t *testing.T) {
					var counter int64
					p := New(rootCtx,
						WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
						WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
						WithCreateItemFunc(func(context.Context) (*testItem, error) {
							atomic.AddInt64(&counter, 1)

							if atomic.LoadInt64(&counter) < 10 {
								return nil, context.Canceled
							}

							var v testItem

							return &v, nil
						}),
					)
					err := p.With(rootCtx, func(ctx context.Context, item *testItem) error {
						return nil
					})
					require.NoError(t, err)
					require.GreaterOrEqual(t, atomic.LoadInt64(&counter), int64(10))
				})
				t.Run("DeadlineExceeded", func(t *testing.T) {
					var counter int64
					p := New(rootCtx,
						WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
						WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
						WithCreateItemFunc(func(context.Context) (*testItem, error) {
							atomic.AddInt64(&counter, 1)

							if atomic.LoadInt64(&counter) < 10 {
								return nil, context.DeadlineExceeded
							}

							var v testItem

							return &v, nil
						}),
					)
					err := p.With(rootCtx, func(ctx context.Context, item *testItem) error {
						return nil
					})
					require.NoError(t, err)
					require.GreaterOrEqual(t, atomic.LoadInt64(&counter), int64(10))
				})
			})
			t.Run("OnTransportError", func(t *testing.T) {
				var counter int64
				p := New(rootCtx,
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						atomic.AddInt64(&counter, 1)

						if atomic.LoadInt64(&counter) < 10 {
							return nil, xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
						}

						var v testItem

						return &v, nil
					}),
				)
				err := p.With(rootCtx, func(ctx context.Context, item *testItem) error {
					return nil
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, atomic.LoadInt64(&counter), int64(10))
			})
			t.Run("OnOperationError", func(t *testing.T) {
				var counter int64
				p := New(rootCtx,
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						atomic.AddInt64(&counter, 1)

						if atomic.LoadInt64(&counter) < 10 {
							return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE))
						}

						var v testItem

						return &v, nil
					}),
				)
				err := p.With(rootCtx, func(ctx context.Context, item *testItem) error {
					return nil
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, atomic.LoadInt64(&counter), int64(10))
			})
			t.Run("NilNil", func(t *testing.T) {
				xtest.TestManyTimes(t, func(t testing.TB) {
					limit := 100
					ctx, cancel := xcontext.WithTimeout(
						context.Background(),
						55*time.Second,
					)
					defer cancel()
					p := New[*testItem, testItem](rootCtx)
					defer func() {
						_ = p.Close(context.Background())
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
					ctx, cancel := context.WithCancel(rootCtx)
					cancel()
					p := New[*testItem, testItem](ctx, WithLimit[*testItem, testItem](1))
					err := p.With(ctx, func(ctx context.Context, testItem *testItem) error {
						return nil
					})
					require.ErrorIs(t, err, context.Canceled)
				})
				t.Run("DeadlineExceeded", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(rootCtx, 0)
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
					ctx, cancel := xcontext.WithCancel(context.Background())
					p := New[*testItem, testItem](ctx, WithLimit[*testItem, testItem](1))

					results := make(chan error)
					go func() {
						err := p.With(ctx,
							func(ctx context.Context, item *testItem) error {
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
					createCounter int64
					closeCounter  int64
				)
				p := New(rootCtx,
					WithLimit[*testItem, testItem](1),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						atomic.AddInt64(&createCounter, 1)

						v := &testItem{
							onClose: func() error {
								atomic.AddInt64(&closeCounter, 1)

								return nil
							},
						}

						return v, nil
					}),
				)
				err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
					return nil
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, atomic.LoadInt64(&createCounter), atomic.LoadInt64(&closeCounter))
				err = p.Close(rootCtx)
				require.NoError(t, err)
				require.EqualValues(t, atomic.LoadInt64(&createCounter), atomic.LoadInt64(&closeCounter))
			})
		})
		t.Run("IsAlive", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					newItems    atomic.Int64
					deleteItems atomic.Int64
					expErr      = errors.New("expected error")
				)
				p := New(rootCtx,
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
				err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
					if newItems.Load() < 10 {
						return xerrors.Retryable(expErr, xerrors.Invalid(testItem))
					}

					return nil
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, newItems.Load(), int64(9))
				require.GreaterOrEqual(t, newItems.Load(), deleteItems.Load())
				err = p.Close(rootCtx)
				require.NoError(t, err)
				require.EqualValues(t, newItems.Load(), deleteItems.Load())
			}, xtest.StopAfter(3*time.Second))
		})
	})
	t.Run("With", func(t *testing.T) {
		t.Run("ItemFromPoolIsNotAlive", func(t *testing.T) {
			var (
				created atomic.Int32
				closed  atomic.Int32
				nextID  atomic.Int32
			)
			assertCreated := func(exp int32) {
				if act := created.Load(); act != exp {
					t.Errorf(
						"unexpected number of created items: %v; want %v",
						act, exp,
					)
				}
			}
			assertClosed := func(exp int32) {
				if act := closed.Load(); act != exp {
					t.Errorf(
						"unexpected number of closed items: %v; want %v",
						act, exp,
					)
				}
			}
			p := New[*testItem, testItem](rootCtx,
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
				_ = p.Close(context.Background())
			}()

			s1 := mustGetItem(t, p)
			assertClosed(0)
			assertCreated(1)
			require.Len(t, p.index, 1)

			mustPutItem(t, p, s1)
			assertClosed(0)
			assertCreated(1)
			require.Len(t, p.index, 1)
			require.Equal(t, 1, p.idle.Len())

			s2, err := p.getItem(context.Background())
			require.NoError(t, err)
			assertCreated(2)
			assertClosed(1)
			require.Len(t, p.index, 1)
			require.Equal(t, 0, p.idle.Len())

			_, err = p.getItem(context.Background())
			require.ErrorIs(t, err, errPoolIsOverflow)
			assertCreated(2)
			assertClosed(1)
			require.Len(t, p.index, 1)
			require.Equal(t, 0, p.idle.Len())

			require.NoError(t, p.Close(context.Background()))
			assertCreated(2)
			assertClosed(1)

			require.Len(t, p.index, 1)
			require.Equal(t, 0, p.idle.Len())

			require.ErrorIs(t, p.putItem(context.Background(), s2), errClosedPool)
			assertClosed(2)

			require.True(t, s2.closed)
			require.False(t, s2.IsAlive())

			require.Len(t, p.index, 0)
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
						"unexpected number of created items: %v; want %v",
						act, exp,
					)
				}
			}
			assertClosed := func(exp int32) {
				if act := closed.Load(); act != exp {
					t.Errorf(
						"unexpected number of closed items: %v; want %v",
						act, exp,
					)
				}
			}
			p := New[*testItem, testItem](rootCtx,
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
				_ = p.Close(context.Background())
			}()

			s := mustGetItem(t, p)
			assertCreated(1)

			mustPutItem(t, p, s)
			assertClosed(0)

			mustGetItem(t, p)
			assertCreated(1)

			p.closeItem(context.Background(), s)
			delete(p.index, s)
			assertClosed(1)

			mustGetItem(t, p)
			assertCreated(2)
		})
		t.Run("OnErrorWithDeleteItem", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					created           atomic.Int32
					errMustDeleteItem = errors.New("item must be deleted")
				)
				p := New[*testItem, testItem](rootCtx,
					WithLimit[*testItem, testItem](1),
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCreateItemFunc(func(context.Context) (*testItem, error) {
						v := testItem{
							v: created.Add(1),
						}

						return &v, nil
					}),
					WithMustDeleteItemFunc[*testItem, testItem](func(item *testItem, err error) bool {
						return errors.Is(err, errMustDeleteItem)
					}),
				)
				defer func() {
					_ = p.Close(context.Background())
				}()

				errThrown := false
				err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
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
		t.Run("Racy", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				trace := &Trace{
					OnChange: func(stats Stats) {
						require.GreaterOrEqual(t, stats.Limit, stats.Idle)
					},
				}
				p := New[*testItem, testItem](rootCtx,
					WithTrace[*testItem, testItem](trace),
				)
				r := xrand.New(xrand.WithLock())
				var wg sync.WaitGroup
				wg.Add(DefaultLimit*2 + 1)
				for range make([]struct{}, DefaultLimit*2) {
					go func() {
						defer wg.Done()
						childCtx, childCancel := xcontext.WithTimeout(
							rootCtx,
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
					err := p.Close(rootCtx)
					require.NoError(t, err)
				}()
				wg.Wait()
			})
		})
		t.Run("ParallelCreation", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				trace := &Trace{
					OnChange: func(stats Stats) {
						require.Equal(t, DefaultLimit, stats.Limit)
						require.LessOrEqual(t, stats.Idle, DefaultLimit)
					},
				}
				p := New[*testItem, testItem](rootCtx,
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithTrace[*testItem, testItem](trace),
				)
				var wg sync.WaitGroup
				for range make([]struct{}, DefaultLimit*10) {
					wg.Add(1)
					go func() {
						defer wg.Done()
						err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
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
		t.Run("PutInFull", func(t *testing.T) {
			p := New(rootCtx,
				WithLimit[*testItem, testItem](1),
				WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
			)
			item := mustGetItem(t, p)
			if err := p.putItem(context.Background(), item); err != nil {
				t.Fatalf("unexpected error on put item into non-full client: %v, wand: %v", err, nil)
			}

			if err := p.putItem(context.Background(), &testItem{}); !xerrors.Is(err, errPoolIsOverflow) {
				t.Fatalf("unexpected error on put item into full pool: %v, wand: %v", err, errPoolIsOverflow)
			}
		})
		t.Run("PutTwice", func(t *testing.T) {
			p := New(rootCtx,
				WithLimit[*testItem, testItem](2),
				WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
				WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
			)
			item := mustGetItem(t, p)
			mustPutItem(t, p, item)

			require.Panics(t, func() {
				_ = p.putItem(context.Background(), item)
			})
		})
	})
}
