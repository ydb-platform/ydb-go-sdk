package conn

import (
	"context"
	"errors"
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

func TestBan(t *testing.T) {
	t.Run("WithBanCallback", func(t *testing.T) {
		var (
			errCause = errors.New("test cause")
			called   bool
		)
		ctx := WithBanCallback(t.Context(), func(cause error) {
			called = true
			require.ErrorIs(t, cause, errCause)
		})
		Ban(ctx, errCause)
		require.True(t, called)
	})

	t.Run("WithoutCallback", func(t *testing.T) {
		require.NotPanics(t, func() {
			Ban(t.Context(), errors.New("test cause"))
		})
	})
}
