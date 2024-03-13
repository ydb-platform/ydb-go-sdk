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

// WithPoolMaxSize defines upper bound of pooled sessions.
// If maxSize is less than or equal to zero then the
// DefaultPoolMaxSize variable is used as a limit.
func WithPoolMaxSize(size int) Option {
	return func(c *Config) {
		if size > 0 {
			c.maxSize = size
		}
	}
}

func WithPoolMinSize(size int) Option {
	return func(c *Config) {
		if size > 0 {
			c.minSize = size
		}
	}
}

func WithPoolProducersCount(count int) Option {
	return func(c *Config) {
		if count > 0 {
			c.producersCount = count
		}
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
