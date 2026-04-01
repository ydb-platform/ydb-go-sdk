package stats_test

import (
	"context"
	"testing"

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
