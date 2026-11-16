package ydb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	queryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
)

func TestSharedSessionPoolConfigReset(t *testing.T) {
	d, err := driverFromOptions(t.Context(),
		WithSessionPoolSessionIdleTimeToLive(time.Second),
		WithSessionPoolSessionIdleTimeToLive(0),
		WithSessionPoolWarmUpSessions(4),
		WithSessionPoolWarmUpSessions(0),
		WithSessionPoolCreateSessionTimeout(time.Second),
		WithSessionPoolCreateSessionTimeout(0),
		WithDisableSessionBalancer(),
	)
	require.NoError(t, err)
	t.Cleanup(d.ctxCancel)
	queryCfg := queryConfig.New(append([]queryConfig.Option{queryConfig.With(d.config.Common)}, d.queryOptions...)...)
	cfg := d.sharedSessionPoolSettings(queryCfg)
	require.Zero(t, cfg.SessionIdleTimeToLive())
	require.Zero(t, cfg.PoolWarmUpSize())
	require.Zero(t, cfg.SessionCreateTimeout())
	require.True(t, cfg.DisableSessionBalancer())
}

func TestSharedSessionPoolConfigLastOption(t *testing.T) {
	d, err := driverFromOptions(t.Context(),
		WithSessionPoolIdleThreshold(0),
		WithSessionPoolSessionIdleTimeToLive(time.Second),
		WithSessionPoolWarmUpSessions(0),
		WithSessionPoolWarmUpSessions(2),
	)
	require.NoError(t, err)
	t.Cleanup(d.ctxCancel)
	cfg := d.sharedSessionPoolSettings(queryConfig.New(queryConfig.With(d.config.Common)))
	require.Equal(t, time.Second, cfg.SessionIdleTimeToLive())
	require.Equal(t, 2, cfg.PoolWarmUpSize())
}

func TestSharedSessionPoolConfigRestoresDefaults(t *testing.T) {
	d, err := driverFromOptions(t.Context(),
		WithSessionPoolSizeLimit(2), WithSessionPoolSizeLimit(0),
		WithSessionPoolDeleteTimeout(time.Second), WithSessionPoolDeleteTimeout(0),
	)
	require.NoError(t, err)
	t.Cleanup(d.ctxCancel)
	queryCfg := queryConfig.New(append([]queryConfig.Option{queryConfig.With(d.config.Common)}, d.queryOptions...)...)
	cfg := d.sharedSessionPoolSettings(queryCfg)
	require.Equal(t, queryConfig.DefaultPoolMaxSize, cfg.PoolLimit())
	require.Equal(t, queryConfig.DefaultSessionDeleteTimeout, cfg.SessionDeleteTimeout())
	require.Equal(t, 2, queryCfg.PoolLimit())
	require.Equal(t, time.Second, queryCfg.SessionDeleteTimeout())
}
