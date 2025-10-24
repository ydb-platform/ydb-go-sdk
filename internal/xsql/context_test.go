package xsql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithExplain(t *testing.T) {
	ctx := context.Background()
	require.False(t, isExplain(ctx))

	ctx = WithExplain(ctx)
	require.True(t, isExplain(ctx))
}

func TestIsExplain(t *testing.T) {
	t.Run("NoValue", func(t *testing.T) {
		ctx := context.Background()
		require.False(t, isExplain(ctx))
	})

	t.Run("WithValue", func(t *testing.T) {
		ctx := WithExplain(context.Background())
		require.True(t, isExplain(ctx))
	})

	t.Run("WithFalseValue", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), ctxExplainQueryModeKey{}, false)
		require.False(t, isExplain(ctx))
	})

	t.Run("WithWrongTypeValue", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), ctxExplainQueryModeKey{}, "true")
		require.False(t, isExplain(ctx))
	})
}
