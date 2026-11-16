package options_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination/options"
)

func TestWithSemaphoreWatch(t *testing.T) {
	t.Run("SetsFlagsAndHandler", func(t *testing.T) {
		called := false
		opt := options.WithSemaphoreWatch(
			options.WatchData|options.WatchOwners,
			func(event options.SemaphoreWatchEvent) {
				called = true
				require.True(t, event.DataChanged)
			},
		)

		cfg := &options.DescribeSemaphoreConfig{}
		opt(cfg)

		require.True(t, cfg.WatchData)
		require.True(t, cfg.WatchOwners)
		require.NotNil(t, cfg.OneShotHandler)

		cfg.OneShotHandler(options.SemaphoreWatchEvent{DataChanged: true})
		require.True(t, called)
	})

	t.Run("WatchDataOnly", func(t *testing.T) {
		cfg := &options.DescribeSemaphoreConfig{}
		options.WithSemaphoreWatch(options.WatchData, func(options.SemaphoreWatchEvent) {})(cfg)
		require.True(t, cfg.WatchData)
		require.False(t, cfg.WatchOwners)
		require.NotNil(t, cfg.OneShotHandler)
	})

	t.Run("WatchOwnersOnly", func(t *testing.T) {
		cfg := &options.DescribeSemaphoreConfig{}
		options.WithSemaphoreWatch(options.WatchOwners, func(options.SemaphoreWatchEvent) {})(cfg)
		require.False(t, cfg.WatchData)
		require.True(t, cfg.WatchOwners)
		require.NotNil(t, cfg.OneShotHandler)
	})

	t.Run("IgnoresZeroFlags", func(t *testing.T) {
		cfg := &options.DescribeSemaphoreConfig{}
		options.WithSemaphoreWatch(0, func(options.SemaphoreWatchEvent) {})(cfg)
		require.False(t, cfg.WatchData)
		require.False(t, cfg.WatchOwners)
		require.Nil(t, cfg.OneShotHandler)
	})

	t.Run("IgnoresNilHandler", func(t *testing.T) {
		cfg := &options.DescribeSemaphoreConfig{}
		options.WithSemaphoreWatch(options.WatchData, nil)(cfg)
		require.False(t, cfg.WatchData)
		require.False(t, cfg.WatchOwners)
		require.Nil(t, cfg.OneShotHandler)
	})
}

func TestDescribeIncludeOptions(t *testing.T) {
	cfg := &options.DescribeSemaphoreConfig{}
	options.WithDescribeOwners(true)(cfg)
	options.WithDescribeWaiters(true)(cfg)
	require.True(t, cfg.IncludeOwners)
	require.True(t, cfg.IncludeWaiters)
}
