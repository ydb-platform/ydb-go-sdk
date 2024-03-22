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

func (t testItem) ID() string {
	return ""
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
			p := New[*testItem, testItem](rootCtx)
			err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
				return nil
			})
			require.NoError(t, err)
		})
		t.Run("WithLimit", func(t *testing.T) {
			p := New[*testItem, testItem](rootCtx, WithLimit[*testItem, testItem](1))
			require.EqualValues(t, 1, p.limit)
		})
		t.Run("WithCreateFunc", func(t *testing.T) {
			var newCounter int64
			p := New(rootCtx,
				WithLimit[*testItem, testItem](1),
				WithCreateFunc(func(context.Context) (*testItem, error) {
					atomic.AddInt64(&newCounter, 1)
					var v testItem

					return &v, nil
				}),
			)
			err := p.With(rootCtx, func(ctx context.Context, item *testItem) error {
				return nil
			})
			require.NoError(t, err)
			require.EqualValues(t, p.limit, atomic.LoadInt64(&newCounter))
		})
	})
	t.Run("Change", func(t *testing.T) {
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
	t.Run("Item", func(t *testing.T) {
		t.Run("Close", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					createCounter int64
					closeCounter  int64
				)
				p := New(rootCtx,
					WithLimit[*testItem, testItem](1),
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
				err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
					return nil
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, atomic.LoadInt64(&createCounter), atomic.LoadInt64(&closeCounter))
				err = p.Close(rootCtx)
				require.NoError(t, err)
				require.EqualValues(t, atomic.LoadInt64(&createCounter), atomic.LoadInt64(&closeCounter))
			}, xtest.StopAfter(time.Second))
		})
		t.Run("IsAlive", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var (
					newItems    int64
					deleteItems int64
					expErr      = xerrors.Retryable(errors.New("expected error"), xerrors.WithDeleteSession())
				)
				p := New(rootCtx,
					WithLimit[*testItem, testItem](1),
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
				err := p.With(rootCtx, func(ctx context.Context, testItem *testItem) error {
					if atomic.LoadInt64(&newItems) < 10 {
						return expErr
					}

					return nil
				})
				require.NoError(t, err)
				require.GreaterOrEqual(t, atomic.LoadInt64(&newItems), int64(9))
				require.GreaterOrEqual(t, atomic.LoadInt64(&newItems), atomic.LoadInt64(&deleteItems))
				err = p.Close(rootCtx)
				require.NoError(t, err)
				require.EqualValues(t, atomic.LoadInt64(&newItems), atomic.LoadInt64(&deleteItems))
			}, xtest.StopAfter(5*time.Second))
		})
	})
	t.Run("Stress", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			p := New[*testItem, testItem](rootCtx)
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
		}, xtest.StopAfter(42*time.Second))
	})
}
