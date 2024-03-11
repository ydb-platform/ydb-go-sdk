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

// WithSizeLimit defines upper bound of pooled sessions.
// If sizeLimit is less than or equal to zero then the
// DefaultPoolMaxSize variable is used as a limit.
func WithSizeLimit(sizeLimit int) Option {
	return func(c *Config) {
		if sizeLimit > 0 {
			c.sizeLimit = sizeLimit
		}
	}
}

// WithCreateSessionTimeout limits maximum time spent on Create session request
// If createSessionTimeout is less than or equal to zero then no used timeout on create session request
func WithCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(c *Config) {
		if createSessionTimeout > 0 {
			c.createSessionTimeout = createSessionTimeout
		} else {
			c.createSessionTimeout = 0
		}
	}
}

// WithDeleteTimeout limits maximum time spent on Delete request
// If deleteTimeout is less than or equal to zero then the DefaultPoolDeleteTimeout is used.
func WithDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(c *Config) {
		if deleteTimeout > 0 {
			c.deleteTimeout = deleteTimeout
		}
	}
}
