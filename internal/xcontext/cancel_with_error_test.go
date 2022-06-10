package xcontext

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCancelWithError(t *testing.T) {
	testError := errors.New("test error")
	t.Run("SimpleCancel", func(t *testing.T) {
		ctx, cancel := WithErrCancel(context.Background())
		cancel(testError)
		require.ErrorIs(t, ctx.Err(), testError)
	})

	t.Run("CancelBeforeParent", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		ctx, cancel := WithErrCancel(parent)

		cancel(testError)
		parentCancel()

		require.ErrorIs(t, ctx.Err(), testError)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("CancelAfterParent", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		ctx, cancel := WithErrCancel(parent)

		parentCancel()
		cancel(testError)

		require.Equal(t, context.Canceled, ctx.Err())
	})

	t.Run("CancelWithNil", func(t *testing.T) {
		ctx, cancel := WithErrCancel(context.Background())
		cancel(nil)
		require.ErrorIs(t, ctx.Err(), errCancelWithNilError)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}
