//go:build go1.21
// +build go1.21

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
	t.Run("CloseDone", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		ctx1, cancel1 := WithDone(ctx, done)
		require.NoError(t, ctx1.Err())
		cancel1()
		require.NoError(t, ctx.Err())
		require.Error(t, ctx1.Err())
	})
}
