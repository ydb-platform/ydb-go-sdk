package config

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(*Config)

// With applies common configuration params
func With(config config.Common) Option {
	return func(c *Config) {
		c.Common = config
	}
}

// WithTrace appends table trace to early defined traces
func WithTrace(trace *trace.Query, opts ...trace.QueryComposeOption) Option {
	return func(c *Config) {
		c.trace = c.trace.Compose(trace, opts...)
	}
}

// WithPoolLimit defines upper bound of pooled sessions.
// If poolLimit is less than or equal to zero then the
// DefaultPoolMaxSize variable is used as a poolLimit.
func WithPoolLimit(size int) Option {
	return func(c *Config) {
		if size > 0 {
			c.poolLimit = size
		}
	}
}

func WithPoolSessionUsageLimit(sessionUsageLimit uint64) Option {
	return func(c *Config) {
		c.poolSessionUsageLimit = sessionUsageLimit
	}
}

// WithSessionCreateTimeout limits maximum time spent on Create session request
// If sessionCreateTimeout is less than or equal to zero then no used timeout on create session request
func WithSessionCreateTimeout(createSessionTimeout time.Duration) Option {
	return func(c *Config) {
		if createSessionTimeout > 0 {
			c.sessionCreateTimeout = createSessionTimeout
		} else {
			c.sessionCreateTimeout = 0
		}
	}
}

// WithSessionDeleteTimeout limits maximum time spent on Delete request
// If sessionDeleteTimeout is less than or equal to zero then the DefaultSessionDeleteTimeout is used.
func WithSessionDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(c *Config) {
		if deleteTimeout > 0 {
			c.sessionDeleteTimeout = deleteTimeout
		}
	}
}

// WithSessionIdleTimeToLive limits maximum time to live of idle session
// If idleTimeToLive is less than or equal to zero then sessions will not be closed by idle
func WithSessionIdleTimeToLive(idleTimeToLive time.Duration) Option {
	return func(c *Config) {
		if idleTimeToLive > 0 {
			c.sessionIddleTimeToLive = idleTimeToLive
		}
	}
}

func WithLazyTx(lazyTx bool) Option {
	return func(c *Config) {
		c.lazyTx = lazyTx
	}
}
