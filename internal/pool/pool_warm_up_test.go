package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestWarmUp(t *testing.T) {
	t.Run("DisabledByDefault", func(t *testing.T) {
		var created atomic.Int32

		p := mustNewPool[*testItem, testItem](t,
			WithCreateItemFunc(func(context.Context) (*testItem, error) {
				created.Add(1)

				return &testItem{}, nil
			}),
		)

		require.Equal(t, 0, p.Stats().Idle)
		require.Equal(t, int32(0), created.Load())
	})

	t.Run("PreCreatesItems", func(t *testing.T) {
		const warmUpSize = 3

		var created atomic.Int32

		p := mustNewPool[*testItem, testItem](t,
			WithLimit[*testItem, testItem](10),
			WithWarmUpItems[*testItem, testItem](warmUpSize),
			WithCreateItemFunc(func(context.Context) (*testItem, error) {
				created.Add(1)

				return &testItem{v: created.Load()}, nil
			}),
		)

		require.Equal(t, warmUpSize, p.Stats().Idle)
		require.Equal(t, int32(warmUpSize), created.Load())
	})

	t.Run("CappedByLimit", func(t *testing.T) {
		const (
			limit      = 2
			warmUpSize = 5
		)

		var created atomic.Int32

		p := mustNewPool[*testItem, testItem](t,
			WithLimit[*testItem, testItem](limit),
			WithWarmUpItems[*testItem, testItem](warmUpSize),
			WithCreateItemFunc(func(context.Context) (*testItem, error) {
				created.Add(1)

				return &testItem{}, nil
			}),
		)

		require.Equal(t, limit, p.Stats().Idle)
		require.Equal(t, int32(limit), created.Load())
	})

	t.Run("CreateErrorFailsNew", func(t *testing.T) {
		createErr := errors.New("warm-up create failed")

		_, err := New[*testItem, testItem](t.Context(),
			WithWarmUpItems[*testItem, testItem](2),
			WithCreateItemFunc(func(context.Context) (*testItem, error) {
				return nil, createErr
			}),
		)
		require.ErrorIs(t, err, createErr)
	})

	t.Run("YdbErrorFailsNew", func(t *testing.T) {
		ydbErr := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE))

		_, err := New[*testItem, testItem](t.Context(),
			WithWarmUpItems[*testItem, testItem](1),
			WithCreateItemFunc(func(context.Context) (*testItem, error) {
				return nil, ydbErr
			}),
		)
		require.Error(t, err)
	})

	t.Run("CancelledContextFailsNew", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		var created atomic.Int32

		_, err := New[*testItem, testItem](ctx,
			WithWarmUpItems[*testItem, testItem](1),
			WithCreateItemFunc(func(context.Context) (*testItem, error) {
				created.Add(1)

				return &testItem{}, nil
			}),
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, int32(0), created.Load())
	})
}
