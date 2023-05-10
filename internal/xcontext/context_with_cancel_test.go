package xcontext

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContextWithCancel(t *testing.T) {
	t.Run("SimpleCancel", func(t *testing.T) {
		ctx, cancel := WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("CancelBeforeParent", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		ctx, cancel := WithCancel(parent)

		cancel()
		parentCancel()

		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("CancelAfterParent", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		ctx, cancel := WithCancel(parent)

		parentCancel()
		cancel()

		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}

func TestContextWithCancelError(t *testing.T) {
	for _, tt := range []struct {
		err error
		str string
	}{
		{
			err: func() error {
				parentCtx, parentCancel := WithCancel(context.Background())
				childCtx, childCancel := WithCancel(parentCtx)
				parentCancel()
				childCancel()
				return childCtx.Err()
			}(),
			str: "context canceled at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext.TestContextWithCancelError.func1(context_with_cancel_test.go:47)`", //nolint:lll
		},
		{
			err: func() error {
				ctx, cancel := WithCancel(context.Background())
				cancel()
				return ctx.Err()
			}(),
			str: "context canceled at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext.TestContextWithCancelError.func2(context_with_cancel_test.go:56)`", //nolint:lll
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.str, tt.err.Error())
		})
	}
}
