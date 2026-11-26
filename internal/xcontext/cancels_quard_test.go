package xcontext

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCancelsGuard(t *testing.T) {
	g := NewCancelsGuard()
	ctx, cancel1 := g.WithCancel(context.Background())
	require.Len(t, g.cancels, 1)
	cancel1()
	require.Error(t, ctx.Err())
	require.Empty(t, g.cancels, 0)
	ctx, _ = g.WithCancel(context.Background())
	require.Len(t, g.cancels, 1)
	ctx, _ = g.WithCancel(ctx)
	require.Len(t, g.cancels, 2)
	g.Cancel()
	require.Error(t, ctx.Err())
}
