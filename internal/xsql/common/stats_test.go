package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

func TestStatsModeContext(t *testing.T) {
	t.Run("nil by default", func(t *testing.T) {
		sm := StatsModeFromContext(context.Background())
		require.Nil(t, sm)
	})

	t.Run("round-trip", func(t *testing.T) {
		called := false
		ctx := WithStatsMode(context.Background(), StatsModeBasic, func(qs stats.QueryStats) {
			called = true
		})
		sm := StatsModeFromContext(ctx)
		require.NotNil(t, sm)
		require.Equal(t, StatsModeBasic, sm.Mode)
		sm.Callback(nil)
		require.True(t, called)
	})

	t.Run("all modes", func(t *testing.T) {
		modes := []StatsMode{
			StatsModeBasic,
			StatsModeFull,
			StatsModeProfile,
		}
		for _, mode := range modes {
			ctx := WithStatsMode(context.Background(), mode, func(qs stats.QueryStats) {})
			sm := StatsModeFromContext(ctx)
			require.NotNil(t, sm)
			require.Equal(t, mode, sm.Mode)
		}
	})
}
