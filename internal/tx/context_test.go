package tx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithLazyTx(t *testing.T) {
	t.Run("SetTrue", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithLazyTx(ctx, true)
		require.True(t, LazyTxFromContext(ctx, false))
	})

	t.Run("SetFalse", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithLazyTx(ctx, false)
		require.False(t, LazyTxFromContext(ctx, true))
	})

	t.Run("DefaultValueTrue", func(t *testing.T) {
		ctx := context.Background()
		require.True(t, LazyTxFromContext(ctx, true))
	})

	t.Run("DefaultValueFalse", func(t *testing.T) {
		ctx := context.Background()
		require.False(t, LazyTxFromContext(ctx, false))
	})

	t.Run("OverrideDefaultTrue", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithLazyTx(ctx, false)
		require.False(t, LazyTxFromContext(ctx, true))
	})

	t.Run("OverrideDefaultFalse", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithLazyTx(ctx, true)
		require.True(t, LazyTxFromContext(ctx, false))
	})
}

func TestWithCommitTx(t *testing.T) {
	t.Run("NotSet", func(t *testing.T) {
		ctx := context.Background()
		require.False(t, CommitTxFromContext(ctx))
	})

	t.Run("Set", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithCommitTx(ctx)
		require.True(t, CommitTxFromContext(ctx))
	})
}
