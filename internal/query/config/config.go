package config

import (
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DefaultPoolDeleteTimeout        = 500 * time.Millisecond
	DefaultPoolCreateSessionTimeout = 5 * time.Second
	DefaultPoolMaxSize              = 50
)

type Config struct {
	config.Common

	sizeLimit int

	createSessionTimeout time.Duration
	deleteTimeout        time.Duration

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
		sizeLimit:            DefaultPoolMaxSize,
		createSessionTimeout: DefaultPoolCreateSessionTimeout,
		deleteTimeout:        DefaultPoolDeleteTimeout,
		clock:                clockwork.NewRealClock(),
		trace:                &trace.Query{},
	}
}

// Trace defines trace over table client calls.
func (c *Config) Trace() *trace.Query {
	return c.trace
}

// Clock defines clock.
func (c *Config) Clock() clockwork.Clock {
	return c.clock
}

// PoolMaxSize is an upper bound of pooled sessions.
// If PoolMaxSize is less than or equal to zero then the
// DefaultPoolMaxSize variable is used as a limit.
func (c *Config) PoolMaxSize() int {
	return c.sizeLimit
}

// CreateSessionTimeout limits maximum time spent on Create session request.
func (c *Config) CreateSessionTimeout() time.Duration {
	return c.createSessionTimeout
}

// DeleteTimeout limits maximum time spent on Delete request
//
// If DeleteTimeout is less than or equal to zero then the DefaultPoolDeleteTimeout is used.
func (c *Config) DeleteTimeout() time.Duration {
	return c.deleteTimeout
}
