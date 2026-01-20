package config

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestNew(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		c := New()
		require.NotNil(t, c)
		require.Equal(t, DefaultSessionPoolSizeLimit, c.SizeLimit())
		require.Equal(t, DefaultSessionPoolSizeLimit, c.NodeLimit())
		require.Equal(t, DefaultSessionPoolCreateSessionTimeout, c.CreateSessionTimeout())
		require.Equal(t, DefaultSessionPoolDeleteTimeout, c.DeleteTimeout())
		require.Equal(t, DefaultSessionPoolIdleThreshold, c.IdleThreshold())
		require.NotNil(t, c.Clock())
		require.NotNil(t, c.Trace())
	})

	t.Run("with nil option", func(t *testing.T) {
		c := New(nil)
		require.NotNil(t, c)
		require.Equal(t, DefaultSessionPoolSizeLimit, c.SizeLimit())
	})
}

func TestWithSizeLimit(t *testing.T) {
	t.Run("positive value", func(t *testing.T) {
		c := New(WithSizeLimit(100))
		require.Equal(t, 100, c.SizeLimit())
	})

	t.Run("zero value uses default", func(t *testing.T) {
		c := New(WithSizeLimit(0), WithNodeLimit(0))
		require.Equal(t, DefaultSessionPoolSizeLimit, c.SizeLimit())
		require.Equal(t, DefaultSessionPoolSizeLimit, c.NodeLimit())
	})

	t.Run("negative value uses default", func(t *testing.T) {
		c := New(WithSizeLimit(-1), WithNodeLimit(-1))
		require.Equal(t, DefaultSessionPoolSizeLimit, c.SizeLimit())
		require.Equal(t, DefaultSessionPoolSizeLimit, c.NodeLimit())
	})
}

func TestWithSessionPoolSessionUsageLimit(t *testing.T) {
	t.Run("uint64 limit", func(t *testing.T) {
		c := New(WithSessionPoolSessionUsageLimit[uint64](1000))
		require.Equal(t, uint64(1000), c.SessionUsageLimit())
		require.Equal(t, time.Duration(0), c.SessionUsageTTL())
	})

	t.Run("duration limit", func(t *testing.T) {
		ttl := 5 * time.Minute
		c := New(WithSessionPoolSessionUsageLimit(ttl))
		require.Equal(t, uint64(0), c.SessionUsageLimit())
		require.Equal(t, ttl, c.SessionUsageTTL())
	})
}

func TestWithKeepAliveMinSize(t *testing.T) {
	t.Run("deprecated function", func(t *testing.T) {
		c := New(WithKeepAliveMinSize(20))
		// This function is deprecated and does nothing
		require.Equal(t, DefaultKeepAliveMinSize, c.KeepAliveMinSize())
	})
}

func TestWithIdleKeepAliveThreshold(t *testing.T) {
	t.Run("deprecated function", func(t *testing.T) {
		c := New(WithIdleKeepAliveThreshold(5))
		// This function is deprecated and does nothing
		require.Equal(t, DefaultIdleKeepAliveThreshold, c.IdleKeepAliveThreshold())
	})
}

func TestWithIdleThreshold(t *testing.T) {
	t.Run("positive value", func(t *testing.T) {
		threshold := 10 * time.Minute
		c := New(WithIdleThreshold(threshold))
		require.Equal(t, threshold, c.IdleThreshold())
	})

	t.Run("zero value", func(t *testing.T) {
		c := New(WithIdleThreshold(0))
		require.Equal(t, time.Duration(0), c.IdleThreshold())
	})

	t.Run("negative value", func(t *testing.T) {
		c := New(WithIdleThreshold(-1 * time.Minute))
		require.Equal(t, time.Duration(0), c.IdleThreshold())
	})
}

func TestWithKeepAliveTimeout(t *testing.T) {
	t.Run("deprecated function", func(t *testing.T) {
		c := New(WithKeepAliveTimeout(1 * time.Second))
		// This function is deprecated and does nothing
		require.Equal(t, DefaultSessionPoolKeepAliveTimeout, c.KeepAliveTimeout())
	})
}

func TestWithCreateSessionTimeout(t *testing.T) {
	t.Run("positive value", func(t *testing.T) {
		timeout := 10 * time.Second
		c := New(WithCreateSessionTimeout(timeout))
		require.Equal(t, timeout, c.CreateSessionTimeout())
	})

	t.Run("zero value", func(t *testing.T) {
		c := New(WithCreateSessionTimeout(0))
		require.Equal(t, time.Duration(0), c.CreateSessionTimeout())
	})

	t.Run("negative value", func(t *testing.T) {
		c := New(WithCreateSessionTimeout(-1 * time.Second))
		require.Equal(t, time.Duration(0), c.CreateSessionTimeout())
	})
}

func TestWithDeleteTimeout(t *testing.T) {
	t.Run("positive value", func(t *testing.T) {
		timeout := 1 * time.Second
		c := New(WithDeleteTimeout(timeout))
		require.Equal(t, timeout, c.DeleteTimeout())
	})

	t.Run("zero value uses default", func(t *testing.T) {
		c := New(WithDeleteTimeout(0))
		require.Equal(t, DefaultSessionPoolDeleteTimeout, c.DeleteTimeout())
	})

	t.Run("negative value uses default", func(t *testing.T) {
		c := New(WithDeleteTimeout(-1 * time.Second))
		require.Equal(t, DefaultSessionPoolDeleteTimeout, c.DeleteTimeout())
	})
}

func TestWithTrace(t *testing.T) {
	t.Run("add trace", func(t *testing.T) {
		tr := &trace.Table{}
		c := New(WithTrace(tr))
		require.NotNil(t, c.Trace())
	})

	t.Run("multiple traces", func(t *testing.T) {
		tr1 := &trace.Table{}
		tr2 := &trace.Table{}
		c := New(WithTrace(tr1), WithTrace(tr2))
		require.NotNil(t, c.Trace())
	})
}

func TestWithIgnoreTruncated(t *testing.T) {
	t.Run("enable ignore truncated", func(t *testing.T) {
		c := New(WithIgnoreTruncated())
		require.True(t, c.IgnoreTruncated())
	})

	t.Run("default is false", func(t *testing.T) {
		c := New()
		require.False(t, c.IgnoreTruncated())
	})
}

func TestWithMaxRequestMessageSize(t *testing.T) {
	t.Run("set max request message size", func(t *testing.T) {
		size := 1024 * 1024
		c := New(WithMaxRequestMessageSize(size))
		require.Equal(t, size, c.MaxRequestMessageSize())
	})

	t.Run("default is zero", func(t *testing.T) {
		c := New()
		require.Equal(t, 0, c.MaxRequestMessageSize())
	})
}

func TestExecuteDataQueryOverQueryService(t *testing.T) {
	t.Run("enable", func(t *testing.T) {
		c := New(ExecuteDataQueryOverQueryService(true))
		require.True(t, c.ExecuteDataQueryOverQueryService())
		require.True(t, c.UseQuerySession())
	})

	t.Run("disable", func(t *testing.T) {
		c := New(ExecuteDataQueryOverQueryService(false))
		require.False(t, c.ExecuteDataQueryOverQueryService())
	})

	t.Run("default is false", func(t *testing.T) {
		c := New()
		require.False(t, c.ExecuteDataQueryOverQueryService())
	})
}

func TestUseQuerySession(t *testing.T) {
	t.Run("enable", func(t *testing.T) {
		c := New(UseQuerySession(true))
		require.True(t, c.UseQuerySession())
	})

	t.Run("disable", func(t *testing.T) {
		c := New(UseQuerySession(false))
		require.False(t, c.UseQuerySession())
	})

	t.Run("default is false", func(t *testing.T) {
		c := New()
		require.False(t, c.UseQuerySession())
	})
}

func TestWithDisableSessionBalancer(t *testing.T) {
	t.Run("disable session balancer", func(t *testing.T) {
		c := New(WithDisableSessionBalancer())
		require.NotNil(t, c)
	})
}

func TestWithClock(t *testing.T) {
	t.Run("custom clock", func(t *testing.T) {
		fakeClock := clockwork.NewFakeClock()
		c := New(WithClock(fakeClock))
		require.Equal(t, fakeClock, c.Clock())
	})

	t.Run("default clock", func(t *testing.T) {
		c := New()
		require.NotNil(t, c.Clock())
	})
}

func TestWith(t *testing.T) {
	t.Run("apply common config", func(t *testing.T) {
		commonCfg := config.Common{}
		c := New(With(commonCfg))
		require.NotNil(t, c)
	})
}

func TestConfigGetters(t *testing.T) {
	t.Run("all getters return expected values", func(t *testing.T) {
		c := New(
			WithSizeLimit(100),
			WithNodeLimit(50),
			WithSessionPoolSessionUsageLimit[uint64](500),
			WithIdleThreshold(3*time.Minute),
			WithCreateSessionTimeout(10*time.Second),
			WithDeleteTimeout(1*time.Second),
			WithIgnoreTruncated(),
			WithMaxRequestMessageSize(2048),
			ExecuteDataQueryOverQueryService(true),
		)

		require.Equal(t, 100, c.SizeLimit())
		require.Equal(t, 50, c.NodeLimit())
		require.Equal(t, uint64(500), c.SessionUsageLimit())
		require.Equal(t, 3*time.Minute, c.IdleThreshold())
		require.Equal(t, 10*time.Second, c.CreateSessionTimeout())
		require.Equal(t, 1*time.Second, c.DeleteTimeout())
		require.True(t, c.IgnoreTruncated())
		require.Equal(t, 2048, c.MaxRequestMessageSize())
		require.True(t, c.ExecuteDataQueryOverQueryService())
		require.True(t, c.UseQuerySession())
		require.NotNil(t, c.Trace())
		require.NotNil(t, c.Clock())
	})
}
