package xquery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
)

func TestWithResponsePartPrefetch(t *testing.T) {
	t.Run("Positive", func(t *testing.T) {
		conn := &Conn{}
		WithResponsePartPrefetch(3)(conn)
		require.Equal(t, 3, conn.responsePartPrefetch)
	})

	t.Run("Zero", func(t *testing.T) {
		conn := &Conn{}
		WithResponsePartPrefetch(0)(conn)
		require.Equal(t, 0, conn.responsePartPrefetch)
	})

	t.Run("Negative", func(t *testing.T) {
		conn := &Conn{}
		WithResponsePartPrefetch(-1)(conn)
		require.Equal(t, 0, conn.responsePartPrefetch)
	})
}

func TestAppendResponsePartPrefetch(t *testing.T) {
	t.Run("Disabled", func(t *testing.T) {
		conn := &Conn{}
		opts := conn.appendResponsePartPrefetch(nil)
		require.Empty(t, opts)
	})

	t.Run("Enabled", func(t *testing.T) {
		conn := &Conn{responsePartPrefetch: 2}
		opts := conn.appendResponsePartPrefetch(nil)
		require.Len(t, opts, 1)

		settings := options.ExecuteSettings(opts...)
		require.Equal(t, 2, settings.ResponsePartPrefetch())
	})
}
