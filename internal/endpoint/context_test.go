package endpoint

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithNodeID(t *testing.T) {
	t.Run("FallbackAllowedByDefault", func(t *testing.T) {
		ctx := WithNodeID(context.Background(), 7)

		_, ok := ctx.Value(ctxEndpointKey{}).(uint32)
		require.True(t, ok)

		nodeID, ok := ContextNodeID(ctx)
		require.True(t, ok)
		require.Equal(t, uint32(7), nodeID)
		require.False(t, ContextDisableFallback(ctx))
	})

	t.Run("DisableFallback", func(t *testing.T) {
		ctx := WithNodeID(context.Background(), 7, WithDisableFallback())

		_, ok := ctx.Value(ctxEndpointKey{}).(ctxPinnedNodeID)
		require.True(t, ok)

		nodeID, ok := ContextNodeID(ctx)
		require.True(t, ok)
		require.Equal(t, uint32(7), nodeID)
		require.True(t, ContextDisableFallback(ctx))
	})
}
