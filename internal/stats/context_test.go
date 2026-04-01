package stats_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

func TestStatsModeContext(t *testing.T) {
	t.Run("nil by default", func(t *testing.T) {
		sm := stats.ModeCallbackFromContext(context.Background())
		require.Nil(t, sm)
	})

	t.Run("round-trip", func(t *testing.T) {
		called := false
		ctx := stats.WithModeCallback(context.Background(), stats.ModeBasic, func(qs stats.QueryStats) {
			called = true
		})
		sm := stats.ModeCallbackFromContext(ctx)
		require.NotNil(t, sm)
		require.Equal(t, stats.ModeBasic, sm.Mode)
		sm.Callback(nil)
		require.True(t, called)
	})

	t.Run("all modes", func(t *testing.T) {
		modes := []stats.Mode{
			stats.ModeBasic,
			stats.ModeFull,
			stats.ModeProfile,
		}
		for _, mode := range modes {
			ctx := stats.WithModeCallback(context.Background(), mode, func(qs stats.QueryStats) {})
			sm := stats.ModeCallbackFromContext(ctx)
			require.NotNil(t, sm)
			require.Equal(t, mode, sm.Mode)
		}
	})
}

func TestStatsModeContextWithDefault(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		var (
			called bool
			ctx    = context.Background()
		)

		sm := stats.ModeCallbackFromContextOverried(ctx, stats.ModeBasic, func(_ stats.QueryStats) {
			called = true
		})

		require.NotNil(t, sm)
		sm.Callback(nil)

		assert.True(t, called)
		assert.Equal(t, stats.ModeBasic, sm.Mode)
	})

	t.Run("defined", func(t *testing.T) {
		var (
			called1, called2 bool
			ctx              = context.Background()
		)

		ctx = stats.WithModeCallback(ctx, stats.ModeFull, func(_ stats.QueryStats) {
			called1 = true
		})

		sm := stats.ModeCallbackFromContextOverried(ctx, stats.ModeBasic, func(_ stats.QueryStats) { called2 = true })
		require.NotNil(t, sm)

		require.Equal(t, stats.ModeFull, sm.Mode)

		sm.Callback(nil)

		require.True(t, called1)
		require.True(t, called2)
	})
}
