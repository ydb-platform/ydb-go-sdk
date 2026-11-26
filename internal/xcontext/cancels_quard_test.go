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

func TestCancelsGuardSeal(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		g := NewCancelsGuard()
		select {
		case <-g.Seal():
		default:
			t.Fatal("a sealed empty guard did not signal immediately")
		}
	})

	for _, force := range []bool{false, true} {
		g := NewCancelsGuard()
		ctx, cancel := g.WithCancel(t.Context())
		drained := g.Seal()
		_, rejected := g.WithCancel(t.Context())
		require.Nil(t, rejected)
		select {
		case <-drained:
			t.Fatal("a sealed guard still has an active context")
		default:
		}

		if force {
			g.Cancel()
		} else {
			cancel()
		}
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		select {
		case <-drained:
		default:
			t.Fatal("a sealed guard did not signal when empty")
		}
		cancel()
		g.Cancel()
	}
}
