package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

func TestWithStatsMode(t *testing.T) {
	t.Run("NotSet", func(t *testing.T) {
		ctx := context.Background()
		mode, callback, ok := StatsModeFromContext(ctx)
		require.False(t, ok)
		require.Zero(t, mode)
		require.Nil(t, callback)
	})

	t.Run("Basic", func(t *testing.T) {
		var called bool
		cb := func(_ stats.QueryStats) { called = true }
		ctx := WithStatsMode(context.Background(), options.StatsModeBasic, cb)

		mode, callback, ok := StatsModeFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, options.StatsModeBasic, mode)
		require.NotNil(t, callback)
		callback(nil)
		require.True(t, called)
	})

	t.Run("Full", func(t *testing.T) {
		cb := func(_ stats.QueryStats) {}
		ctx := WithStatsMode(context.Background(), options.StatsModeFull, cb)

		mode, callback, ok := StatsModeFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, options.StatsModeFull, mode)
		require.NotNil(t, callback)
	})

	t.Run("Profile", func(t *testing.T) {
		cb := func(_ stats.QueryStats) {}
		ctx := WithStatsMode(context.Background(), options.StatsModeProfile, cb)

		mode, callback, ok := StatsModeFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, options.StatsModeProfile, mode)
		require.NotNil(t, callback)
	})

	t.Run("NilCallback", func(t *testing.T) {
		ctx := WithStatsMode(context.Background(), options.StatsModeBasic, nil)

		mode, callback, ok := StatsModeFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, options.StatsModeBasic, mode)
		require.Nil(t, callback)
	})

	t.Run("WrongType", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), ctxStatsModeKey{}, "wrong type")
		_, _, ok := StatsModeFromContext(ctx)
		require.False(t, ok)
	})
}
