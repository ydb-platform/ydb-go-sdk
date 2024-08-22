package pool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"runtime"
	"runtime/debug"
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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

type testItem struct {
	v uint32

	closed bytes.Buffer

	onClose   func() error
	onIsAlive func() bool
}

func (t *testItem) IsAlive() bool {
	if t.onIsAlive != nil {
		return t.onIsAlive()
	}

	return true
}

func (t *testItem) ID() string {
	return ""
}

func (t *testItem) Close(context.Context) error {
	if t.closed.Len() > 0 {
		debug.PrintStack()
		fmt.Println(t.closed.String())
		panic("item already closed")
	}

	t.closed.Write(debug.Stack())

	if t.onClose != nil {
		return t.onClose()
	}

	return nil
}

func caller() string {
	_, file, line, _ := runtime.Caller(2)

	return fmt.Sprintf("%s:%d", path.Base(file), line)
}

func mustGetItem[PT Item[T], T any](t testing.TB, p *Pool[PT, T]) PT {
	s, err := p.getItem(context.Background())
	if err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}

	return s
}

func mustPutItem[PT Item[T], T any](t testing.TB, p *Pool[PT, T], item PT) {
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

func TestPool(t *testing.T) {
	rootCtx := xtest.Context(t)
	t.Run("New", func(t *testing.T) {
		t.Run("Default", func(t *testing.T) {
			p := New[*testItem, testItem](rootCtx)
			err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
				return nil
			})
			require.NoError(t, err)
		})
		t.Run("WithLimit", func(t *testing.T) {
			p := New[*testItem, testItem](rootCtx, WithLimit[*testItem, testItem](1))
			require.EqualValues(t, 1, p.config.limit)
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
			)
			err := p.With(rootCtx, func(ctx context.Context, item *testItem) error {
				return nil
			})
			require.NoError(t, err)
			require.EqualValues(t, p.config.limit, atomic.LoadInt64(&newCounter))
		})
		t.Run("ParallelCreation", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				trace := *defaultTrace
				trace.OnChange = func(info ChangeInfo) {
					require.Equal(t, DefaultLimit, info.Limit)
					require.LessOrEqual(t, info.Idle, DefaultLimit)
				}
				p := New[*testItem, testItem](rootCtx,
					WithCreateItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithCloseItemTimeout[*testItem, testItem](50*time.Millisecond),
					WithTrace[*testItem, testItem](&trace),
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
				// replace default async closer for sync testing
				withCloseItemFunc(func(ctx context.Context, item *testItem) {
					_ = item.Close(ctx)
				}),
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
	})
	t.Run("TestSessionPoolCloseIdleSessions", func(t *testing.T) {
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
				// replace default async closer for sync testing
				withCloseItemFunc[*testItem, testItem](func(ctx context.Context, item *testItem) {
					_ = item.Close(ctx)
				}),
				WithClock[*testItem, testItem](fakeClock),
				WithIdleThreshold[*testItem, testItem](idleThreshold),
			)

			s1 := mustGetItem(t, p)
			s2 := mustGetItem(t, p)

			// Put both items at the absolutely same time.
			// That is, both items must be updated their touched timestamp.
			mustPutItem(t, p, s1)
			mustPutItem(t, p, s2)

			require.Len(t, p.index, 2)
			require.Equal(t, 2, p.idle.Len())

			// Move clock to longer than idleThreshold
			fakeClock.Advance(idleThreshold + time.Nanosecond)

			// on get item from idle list the pool must check the item idle timestamp
			// both existing items must be closed
			// getItem must create a new item and return it from getItem
			s3 := mustGetItem(t, p)

			require.Len(t, p.index, 1)

			if !closedCount.CompareAndSwap(2, 0) {
				t.Fatal("unexpected number of closed items")
			}

			// Move time to idleThreshold / 2 - this emulate a "spent" some time working within item.
			fakeClock.Advance(idleThreshold / 2)

			// Now put that item back
			// pool must update a touched timestamp of item
			mustPutItem(t, p, s3)

			// Move time to idleThreshold / 2
			// Total time since last updating touched timestampe is more than idleThreshold
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
					// replace default async closer for sync testing
					withCloseItemFunc(func(ctx context.Context, item *testItem) {
						_ = item.Close(ctx)
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
					expErr      = xerrors.Retryable(errors.New("expected error"), xerrors.InvalidObject())
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
					// replace default async closer for sync testing
					withCloseItemFunc(func(ctx context.Context, item *testItem) {
						_ = item.Close(ctx)
					}),
				)
				err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
					if newItems.Load() < 10 {
						return expErr
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
		t.Run("ExplicitSessionClose", func(t *testing.T) {
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
				// replace default async closer for sync testing
				withCloseItemFunc(func(ctx context.Context, item *testItem) {
					_ = item.Close(ctx)
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
		t.Run("Stress", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				trace := *defaultTrace
				trace.OnChange = func(info ChangeInfo) {
					require.GreaterOrEqual(t, info.Limit, info.Idle)
				}
				p := New[*testItem, testItem](rootCtx,
					WithTrace[*testItem, testItem](&trace),
					// replace default async closer for sync testing
					withCloseItemFunc(func(ctx context.Context, item *testItem) {
						_ = item.Close(ctx)
					}),
				)
				var wg sync.WaitGroup
				wg.Add(DefaultLimit*2 + 1)
				for range make([]struct{}, DefaultLimit*2) {
					go func() {
						defer wg.Done()
						err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
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
	})
}
