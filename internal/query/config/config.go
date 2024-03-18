package config

import (
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DefaultSessionDeleteTimeout = 500 * time.Millisecond
	DefaultSessionCreateTimeout = 5 * time.Second
	DefaultPoolMinSize          = pool.DefaultMinSize
	DefaultPoolMaxSize          = pool.DefaultMaxSize
	DefaultPoolProducersCount   = pool.DefaultProducersCount
)

type Config struct {
	config.Common

	minSize        int
	maxSize        int
	producersCount int

	sessionCreateTimeout time.Duration
	sessionDeleteTimeout time.Duration

	trace *trace.Query

	clock clockwork.Clock
}

func New(opts ...Option) *Config {
	c := defaults()
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	return c
}

func defaults() *Config {
	return &Config{
		minSize:              DefaultPoolMinSize,
		maxSize:              DefaultPoolMaxSize,
		producersCount:       DefaultPoolProducersCount,
		sessionCreateTimeout: DefaultSessionCreateTimeout,
		sessionDeleteTimeout: DefaultSessionDeleteTimeout,
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

func (c *Config) PoolMinSize() int {
	return c.minSize
}

// PoolMaxSize is an upper bound of pooled sessions.
// If PoolMaxSize is less than or equal to zero then the
// DefaultPoolMaxSize variable is used as a limit.
func (c *Config) PoolMaxSize() int {
	return c.maxSize
}

func (c *Config) PoolProducersCount() int {
	return c.producersCount
}

// SessionCreateTimeout limits maximum time spent on Create session request
func (c *Config) SessionCreateTimeout() time.Duration {
	return c.sessionCreateTimeout
}

// DeleteTimeout limits maximum time spent on Delete request
//
// If DeleteTimeout is less than or equal to zero then the DefaultSessionDeleteTimeout is used.
func (c *Config) DeleteTimeout() time.Duration {
	return c.sessionDeleteTimeout
}
