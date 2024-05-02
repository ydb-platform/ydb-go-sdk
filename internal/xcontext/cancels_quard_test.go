package xcontext

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestCancelsGuard(t *testing.T) {
	g := NewCancelsGuard()
	ctx, cancel1 := context.WithCancel(context.Background())
	g.Remember(&cancel1)
	require.Len(t, g.cancels, 1)
	g.Forget(&cancel1)
	require.Empty(t, g.cancels, 0)
	cancel2 := context.CancelFunc(func() {
		cancel1()
	})
	g.Remember(&cancel2)
	require.Len(t, g.cancels, 1)
	g.Cancel()
	require.Error(t, ctx.Err())
}
