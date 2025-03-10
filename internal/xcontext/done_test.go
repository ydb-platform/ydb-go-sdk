package xcontext

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithDone(t *testing.T) {
	t.Run("WithParentCancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		ctx1, _ := WithDone(ctx, done)
		require.NoError(t, ctx1.Err())
		cancel()
		require.Error(t, ctx1.Err())
	})
	t.Run("WithExplicitCancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		ctx1, cancel1 := WithDone(ctx, done)
		require.NoError(t, ctx1.Err())
		cancel1()
		require.NoError(t, ctx.Err())
		require.Error(t, ctx1.Err())
	})
	t.Run("WithExplicitCloseDone", func(t *testing.T) {
		done := make(chan struct{})
		ctx, cancel := WithDone(context.Background(), done)
		require.NoError(t, ctx.Err())
		close(done)
		<-ctx.Done()
		require.Error(t, ctx.Err())
		cancel()
	})
	t.Run("WithClosedDone", func(t *testing.T) {
		done := make(chan struct{})
		close(done)
		ctx, cancel := WithDone(context.Background(), done)
		require.Error(t, ctx.Err())
		cancel()
	})
	t.Run("WithNilDone", func(t *testing.T) {
		var done chan struct{}
		ctx, cancel := WithDone(context.Background(), done)
		require.NoError(t, ctx.Err())
		cancel()
		require.Error(t, ctx.Err())
	})
}
