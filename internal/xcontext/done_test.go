package xcontext

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithDone(t *testing.T) {
	t.Run("CancelParent", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		ctx1, _ := WithDone(ctx, done)
		require.NoError(t, ctx1.Err())
		cancel()
		require.Error(t, ctx1.Err())
	})
	t.Run("StopWithDone", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		ctx1, cancel1 := WithDone(ctx, done)
		require.NoError(t, ctx1.Err())
		cancel1()
		require.NoError(t, ctx.Err())
		require.Error(t, ctx1.Err())
	})
	t.Run("CloseDone", func(t *testing.T) {
		done := make(chan struct{})
		ctx, cancel := WithDone(context.Background(), done)
		require.NoError(t, ctx.Err())
		close(done)
		<-ctx.Done()
		require.Error(t, ctx.Err())
		cancel()
	})
	t.Run("ClosedDone", func(t *testing.T) {
		done := make(chan struct{})
		close(done)
		ctx, cancel := WithDone(context.Background(), done)
		require.Error(t, ctx.Err())
		cancel()
	})
	t.Run("NilDone", func(t *testing.T) {
		var done chan struct{}
		ctx, cancel := WithDone(context.Background(), done)
		require.NoError(t, ctx.Err())
		cancel()
		require.Error(t, ctx.Err())
	})
}
