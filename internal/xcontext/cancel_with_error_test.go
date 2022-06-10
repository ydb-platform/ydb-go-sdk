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
		require.Equal(t, testError, ctx.Err())
	})

	t.Run("CancelBeforeParent", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		ctx, cancel := WithErrCancel(parent)

		cancel(testError)
		parentCancel()

		require.Equal(t, testError, ctx.Err())
	})

	t.Run("CancelAfterParent", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		ctx, cancel := WithErrCancel(parent)

		parentCancel()
		cancel(testError)

		require.Equal(t, context.Canceled, ctx.Err())
	})

}
