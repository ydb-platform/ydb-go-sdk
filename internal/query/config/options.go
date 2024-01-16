package config

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
)

type Option func(*Config)

// With applies common configuration params
func With(config config.Common) Option {
	return func(c *Config) {
		c.Common = config
	}
}

// WithSizeLimit defines upper bound of pooled sessions.
// If sizeLimit is less than or equal to zero then the
// DefaultSessionPoolSizeLimit variable is used as a limit.
func WithSizeLimit(sizeLimit int) Option {
	return func(c *Config) {
		if sizeLimit > 0 {
			c.sizeLimit = sizeLimit
		}
	}
}

// WithIdleThreshold sets maximum duration between any activity within session.
// If this threshold reached, session will be closed.
//
// If idleThreshold is less than zero then there is no idle limit.
// If idleThreshold is zero, then the DefaultSessionPoolIdleThreshold value is used.
func WithIdleThreshold(idleThreshold time.Duration) Option {
	return func(c *Config) {
		if idleThreshold < 0 {
			idleThreshold = 0
		}
		c.idleThreshold = idleThreshold
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
// If deleteTimeout is less than or equal to zero then the DefaultSessionPoolDeleteTimeout is used.
func WithDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(c *Config) {
		if deleteTimeout > 0 {
			c.deleteTimeout = deleteTimeout
		}
	}
}
