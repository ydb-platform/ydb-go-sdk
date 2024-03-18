package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

type testItem struct {
	v uint32

	onClose   func() error
	onIsAlive func() bool
}

func (t testItem) IsAlive() bool {
	if t.onIsAlive != nil {
		return t.onIsAlive()
	}

	return true
}

func (t testItem) Close(context.Context) error {
	if t.onClose != nil {
		return t.onClose()
	}

	return nil
}

func TestPool(t *testing.T) {
	rootCtx := xtest.Context(t)
	t.Run("New", func(t *testing.T) {
		t.Run("Default", func(t *testing.T) {
			p, err := New[*testItem, testItem](rootCtx)
			require.NoError(t, err)
			err = p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
				return nil
			})
			require.NoError(t, err)
		})
		t.Run("WithMaxSize", func(t *testing.T) {
			p, err := New[*testItem, testItem](rootCtx, WithMaxSize[*testItem, testItem](1))
			require.NoError(t, err)
			require.EqualValues(t, 1, p.maxSize)
		})
		t.Run("WithMinSize", func(t *testing.T) {
			t.Run("LessOrEqualMaxSize", func(t *testing.T) {
				p, err := New[*testItem, testItem](rootCtx,
					WithMinSize[*testItem, testItem](1),
				)
				require.NoError(t, err)
				require.EqualValues(t, p.minSize, 1)
			})
			t.Run("GreatThenMaxSize", func(t *testing.T) {
				p, err := New[*testItem, testItem](rootCtx, WithMinSize[*testItem, testItem](100))
				require.NoError(t, err)
				require.EqualValues(t, DefaultMaxSize/10, p.minSize)
			})
		})
		t.Run("WithCreateFunc", func(t *testing.T) {
			var newCounter int64
			p, err := New(rootCtx,
				WithCreateFunc(func(context.Context) (*testItem, error) {
					atomic.AddInt64(&newCounter, 1)
					var v testItem

					return &v, nil
				}),
			)
			require.NoError(t, err)
			require.EqualValues(t, p.minSize, atomic.LoadInt64(&newCounter))
		})
	})
	t.Run("With", func(t *testing.T) {
		t.Run("Context", func(t *testing.T) {
			t.Run("Canceled", func(t *testing.T) {
				ctx, cancel := context.WithCancel(rootCtx)
				cancel()
				p, err := New[*testItem, testItem](ctx, WithMaxSize[*testItem, testItem](1))
				require.NoError(t, err)
				err = p.With(ctx, func(ctx context.Context, testItem *testItem) error {
					return nil
				})
				require.ErrorIs(t, err, context.Canceled)
			})
			t.Run("DeadlineExceeded", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(rootCtx, 0)
				cancel()
				p, err := New[*testItem, testItem](ctx, WithMaxSize[*testItem, testItem](1))
				require.NoError(t, err)
				err = p.With(ctx, func(ctx context.Context, testItem *testItem) error {
					return nil
				})
				require.ErrorIs(t, err, context.DeadlineExceeded)
			})
		})
	})
	t.Run("Item", func(t *testing.T) {
		t.Run("Close", func(t *testing.T) {
			xtest.TestManyTimes(t, func(tb testing.TB) {
				tb.Helper()
				var (
					createCounter int64
					closeCounter  int64
				)
				p, err := New(rootCtx,
					WithMinSize[*testItem, testItem](0),
					WithMaxSize[*testItem, testItem](1),
					WithCreateFunc(func(context.Context) (*testItem, error) {
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
				require.NoError(tb, err)
				err = p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
					return nil
				})
				require.NoError(tb, err)
				require.GreaterOrEqual(tb, atomic.LoadInt64(&createCounter), atomic.LoadInt64(&closeCounter))
				p.Close(rootCtx)
				require.EqualValues(tb, atomic.LoadInt64(&createCounter), atomic.LoadInt64(&closeCounter))
			}, xtest.StopAfter(time.Second))
		})
		t.Run("IsAlive", func(t *testing.T) {
			xtest.TestManyTimes(t, func(tb testing.TB) {
				tb.Helper()
				var (
					newItems    int64
					deleteItems int64
					expErr      = xerrors.Retryable(errors.New("expected error"), xerrors.WithDeleteSession())
				)
				p, err := New(rootCtx,
					WithMaxSize[*testItem, testItem](1),
					WithCreateFunc(func(context.Context) (*testItem, error) {
						atomic.AddInt64(&newItems, 1)

						v := &testItem{
							onClose: func() error {
								atomic.AddInt64(&deleteItems, 1)

								return nil
							},
							onIsAlive: func() bool {
								return atomic.LoadInt64(&newItems) >= 10
							},
						}

						return v, nil
					}),
				)
				require.NoError(tb, err)
				err = p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
					if atomic.LoadInt64(&newItems) < 10 {
						return expErr
					}

					return nil
				})
				require.NoError(tb, err)
				require.GreaterOrEqual(tb, atomic.LoadInt64(&newItems), int64(9))
				require.GreaterOrEqual(tb, atomic.LoadInt64(&newItems), atomic.LoadInt64(&deleteItems))
				p.Close(rootCtx)
				require.EqualValues(tb, atomic.LoadInt64(&newItems), atomic.LoadInt64(&deleteItems))
			}, xtest.StopAfter(5*time.Second))
		})
	})
	t.Run("Stress", func(t *testing.T) {
		xtest.TestManyTimes(t, func(tb testing.TB) {
			tb.Helper()
			p, err := New[*testItem, testItem](rootCtx, WithMinSize[*testItem, testItem](DefaultMaxSize/2))
			require.NoError(tb, err)
			var wg sync.WaitGroup
			wg.Add(DefaultMaxSize*2 + 1)
			for range make([]struct{}, DefaultMaxSize*2) {
				go func() {
					defer wg.Done()
					err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
						return nil
					})
					if err != nil && !xerrors.Is(err, errClosedPool, context.Canceled) {
						tb.Failed()
					}
				}()
			}
			go func() {
				defer wg.Done()
				time.Sleep(time.Millisecond)
				err := p.Close(rootCtx)
				require.NoError(tb, err)
			}()
			wg.Wait()
		}, xtest.StopAfter(42*time.Second))
	})
}
