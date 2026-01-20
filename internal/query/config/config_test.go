package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestNew(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		cfg := New()
		require.NotNil(t, cfg)
		require.Equal(t, DefaultPoolMaxSize, cfg.PoolLimit())
		require.Equal(t, DefaultPoolMaxSize, cfg.NodeLimit())
		require.Equal(t, DefaultSessionCreateTimeout, cfg.SessionCreateTimeout())
		require.Equal(t, DefaultSessionDeleteTimeout, cfg.SessionDeleteTimeout())
		require.NotNil(t, cfg.Trace())
		require.False(t, cfg.AllowImplicitSessions())
		require.Equal(t, uint64(0), cfg.PoolSessionUsageLimit())
		require.Equal(t, time.Duration(0), cfg.PoolSessionUsageTTL())
		require.Equal(t, time.Duration(0), cfg.SessionIdleTimeToLive())
		require.False(t, cfg.LazyTx())
	})

	t.Run("WithPoolLimit", func(t *testing.T) {
		cfg := New(WithPoolLimit(100), WithNodeLimit(50))
		require.Equal(t, 100, cfg.PoolLimit())
		require.Equal(t, 50, cfg.NodeLimit())
	})

	t.Run("WithPoolLimitZero", func(t *testing.T) {
		cfg := New(WithPoolLimit(0), WithNodeLimit(0))
		require.Equal(t, DefaultPoolMaxSize, cfg.PoolLimit())
		require.Equal(t, DefaultPoolMaxSize, cfg.NodeLimit())

	})

	t.Run("WithPoolLimitNegative", func(t *testing.T) {
		cfg := New(WithPoolLimit(-1), WithNodeLimit(-1))
		require.Equal(t, DefaultPoolMaxSize, cfg.PoolLimit())
		require.Equal(t, DefaultPoolMaxSize, cfg.NodeLimit())
	})

	t.Run("WithSessionPoolSessionUsageLimit-Uint64", func(t *testing.T) {
		cfg := New(WithSessionPoolSessionUsageLimit(uint64(50)))
		require.Equal(t, uint64(50), cfg.PoolSessionUsageLimit())
	})

	t.Run("WithSessionPoolSessionUsageLimit-Duration", func(t *testing.T) {
		cfg := New(WithSessionPoolSessionUsageLimit(5 * time.Minute))
		require.Equal(t, 5*time.Minute, cfg.PoolSessionUsageTTL())
	})

	t.Run("WithSessionCreateTimeout", func(t *testing.T) {
		cfg := New(WithSessionCreateTimeout(2 * time.Second))
		require.Equal(t, 2*time.Second, cfg.SessionCreateTimeout())
	})

	t.Run("WithSessionCreateTimeoutZero", func(t *testing.T) {
		cfg := New(WithSessionCreateTimeout(0))
		require.Equal(t, time.Duration(0), cfg.SessionCreateTimeout())
	})

	t.Run("WithSessionDeleteTimeout", func(t *testing.T) {
		cfg := New(WithSessionDeleteTimeout(1 * time.Second))
		require.Equal(t, 1*time.Second, cfg.SessionDeleteTimeout())
	})

	t.Run("WithSessionDeleteTimeoutZero", func(t *testing.T) {
		cfg := New(WithSessionDeleteTimeout(0))
		require.Equal(t, DefaultSessionDeleteTimeout, cfg.SessionDeleteTimeout())
	})

	t.Run("WithSessionIdleTimeToLive", func(t *testing.T) {
		cfg := New(WithSessionIdleTimeToLive(10 * time.Minute))
		require.Equal(t, 10*time.Minute, cfg.SessionIdleTimeToLive())
	})

	t.Run("WithSessionIdleTimeToLiveZero", func(t *testing.T) {
		cfg := New(WithSessionIdleTimeToLive(0))
		require.Equal(t, time.Duration(0), cfg.SessionIdleTimeToLive())
	})

	t.Run("AllowImplicitSessions", func(t *testing.T) {
		cfg := New(AllowImplicitSessions())
		require.True(t, cfg.AllowImplicitSessions())
	})

	t.Run("WithLazyTx", func(t *testing.T) {
		cfg := New(WithLazyTx(true))
		require.True(t, cfg.LazyTx())
	})

	t.Run("WithTrace", func(t *testing.T) {
		tr := &trace.Query{
			OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(info trace.QuerySessionCreateDoneInfo) {
				return func(info trace.QuerySessionCreateDoneInfo) {}
			},
		}
		cfg := New(WithTrace(tr))
		require.NotNil(t, cfg.Trace())
	})

	t.Run("NilOption", func(t *testing.T) {
		cfg := New(nil)
		require.NotNil(t, cfg)
	})

	t.Run("MultipleOptions", func(t *testing.T) {
		cfg := New(
			WithPoolLimit(200),
			WithSessionCreateTimeout(3*time.Second),
			WithSessionDeleteTimeout(2*time.Second),
			AllowImplicitSessions(),
			WithLazyTx(true),
		)
		require.Equal(t, 200, cfg.PoolLimit())
		require.Equal(t, 3*time.Second, cfg.SessionCreateTimeout())
		require.Equal(t, 2*time.Second, cfg.SessionDeleteTimeout())
		require.True(t, cfg.AllowImplicitSessions())
		require.True(t, cfg.LazyTx())
	})
}

func TestDefaults(t *testing.T) {
	cfg := defaults()
	require.NotNil(t, cfg)
	require.Equal(t, DefaultPoolMaxSize, cfg.poolLimit)
	require.Equal(t, DefaultSessionCreateTimeout, cfg.sessionCreateTimeout)
	require.Equal(t, DefaultSessionDeleteTimeout, cfg.sessionDeleteTimeout)
	require.NotNil(t, cfg.trace)
}
