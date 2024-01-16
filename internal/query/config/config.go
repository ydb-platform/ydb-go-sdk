package config

import (
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DefaultSessionPoolDeleteTimeout        = 500 * time.Millisecond
	DefaultSessionPoolCreateSessionTimeout = 5 * time.Second
	DefaultSessionPoolSizeLimit            = 50
	DefaultSessionPoolIdleThreshold        = 5 * time.Minute
)

type Config struct {
	config.Common

	sizeLimit int

	createSessionTimeout time.Duration
	deleteTimeout        time.Duration
	idleThreshold        time.Duration

	trace *trace.Query

	clock clockwork.Clock
}

func New(opts ...Option) *Config {
	c := defaults()
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	return c
}

func defaults() *Config {
	return &Config{
		sizeLimit:            DefaultSessionPoolSizeLimit,
		createSessionTimeout: DefaultSessionPoolCreateSessionTimeout,
		deleteTimeout:        DefaultSessionPoolDeleteTimeout,
		idleThreshold:        DefaultSessionPoolIdleThreshold,
		clock:                clockwork.NewRealClock(),
		trace:                &trace.Query{},
	}
}

// Trace defines trace over table client calls
func (c *Config) Trace() *trace.Query {
	return c.trace
}

// Clock defines clock
func (c *Config) Clock() clockwork.Clock {
	return c.clock
}

// SizeLimit is an upper bound of pooled sessions.
// If SizeLimit is less than or equal to zero then the
// DefaultSessionPoolSizeLimit variable is used as a limit.
func (c *Config) SizeLimit() int {
	return c.sizeLimit
}

// IdleThreshold is a maximum duration between any activity within session.
// If this threshold reached, idle session will be closed
//
// If IdleThreshold is less than zero then there is no idle limit.
// If IdleThreshold is zero, then the DefaultSessionPoolIdleThreshold value is used.
func (c *Config) IdleThreshold() time.Duration {
	return c.idleThreshold
}

// CreateSessionTimeout limits maximum time spent on Create session request
func (c *Config) CreateSessionTimeout() time.Duration {
	return c.createSessionTimeout
}

// DeleteTimeout limits maximum time spent on Delete request
//
// If DeleteTimeout is less than or equal to zero then the DefaultSessionPoolDeleteTimeout is used.
func (c *Config) DeleteTimeout() time.Duration {
	return c.deleteTimeout
}
