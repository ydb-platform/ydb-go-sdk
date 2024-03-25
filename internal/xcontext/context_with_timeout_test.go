package xcontext

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestContextWithTimeout(t *testing.T) {
	t.Run("SimpleCancel", func(t *testing.T) {
		ctx, cancel := WithTimeout(context.Background(), time.Hour)
		cancel()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("CancelBeforeParent", func(t *testing.T) {
		parent, parentCancel := context.WithTimeout(context.Background(), time.Hour)
		ctx, cancel := WithTimeout(parent, time.Hour)

		cancel()
		parentCancel()

		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("CancelAfterParent", func(t *testing.T) {
		parent, parentCancel := context.WithTimeout(context.Background(), time.Hour)
		ctx, cancel := WithTimeout(parent, time.Hour)

		parentCancel()
		cancel()

		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}

func TestContextWithTimeoutError(t *testing.T) {
	for _, tt := range []struct {
		err error
		str string
	}{
		{
			err: func() error {
				parentCtx, parentCancel := WithTimeout(context.Background(), time.Hour)
				childCtx, childCancel := WithTimeout(parentCtx, time.Hour)
				parentCancel()
				childCancel()

				return childCtx.Err()
			}(),
			str: "'context canceled' at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext.TestContextWithTimeoutError.func1(context_with_timeout_test.go:48)`", //nolint:lll
		},
		{
			err: func() error {
				ctx, cancel := WithTimeout(context.Background(), time.Hour)
				cancel()

				return ctx.Err()
			}(),
			str: "'context canceled' at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext.TestContextWithTimeoutError.func2(context_with_timeout_test.go:58)`", //nolint:lll
		},
		{
			err: func() error {
				parentCtx, _ := WithTimeout(context.Background(), 0)
				childCtx, _ := WithTimeout(parentCtx, 0)

				return childCtx.Err()
			}(),
			str: "'context deadline exceeded' from `github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext.TestContextWithTimeoutError.func3(context_with_timeout_test.go:66)`", //nolint:lll
		},
		{
			err: func() error {
				ctx, _ := WithTimeout(context.Background(), 0)

				return ctx.Err()
			}(),
			str: "'context deadline exceeded' from `github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext.TestContextWithTimeoutError.func4(context_with_timeout_test.go:75)`", //nolint:lll
		},
		{
			err: func() error {
				parentCtx, cancel := WithCancel(context.Background())
				childCtx, _ := WithTimeout(parentCtx, 0)
				cancel()

				return childCtx.Err()
			}(),
			str: "'context canceled' at `github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext.TestContextWithTimeoutError.func5(context_with_timeout_test.go:85)`", //nolint:lll
		},
		{
			err: func() error {
				parentCtx, cancel := context.WithCancel(context.Background())
				childCtx, _ := WithTimeout(parentCtx, 0)
				cancel()

				return childCtx.Err()
			}(),
			str: "context canceled",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.str, tt.err.Error())
		})
	}
}
