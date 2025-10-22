package conn

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithoutWrapping(t *testing.T) {
	t.Run("ContextWithoutWrapping", func(t *testing.T) {
		ctx := context.Background()
		ctxWithoutWrapping := WithoutWrapping(ctx)
		require.NotNil(t, ctxWithoutWrapping)
		require.False(t, UseWrapping(ctxWithoutWrapping))
	})
}

func TestUseWrapping(t *testing.T) {
	t.Run("DefaultContextUsesWrapping", func(t *testing.T) {
		ctx := context.Background()
		require.True(t, UseWrapping(ctx))
	})

	t.Run("ContextWithoutWrappingDoesNotUseWrapping", func(t *testing.T) {
		ctx := WithoutWrapping(context.Background())
		require.False(t, UseWrapping(ctx))
	})

	t.Run("NestedContextWithoutWrapping", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithoutWrapping(ctx)
		ctx = context.WithValue(ctx, "key", "value") //nolint:revive,staticcheck
		
		require.False(t, UseWrapping(ctx))
	})
}
