package config

import (
	"os"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DefaultSessionDeleteTimeout = 500 * time.Millisecond
	DefaultSessionCreateTimeout = 500 * time.Millisecond
	DefaultPoolMaxSize          = pool.DefaultLimit
)

type Config struct {
	config.Common

	poolLimit int

	useSessionPool       bool
	sessionCreateTimeout time.Duration
	sessionDeleteTimeout time.Duration

	trace *trace.Query
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
		poolLimit:            DefaultPoolMaxSize,
		sessionCreateTimeout: DefaultSessionCreateTimeout,
		sessionDeleteTimeout: DefaultSessionDeleteTimeout,
		trace:                &trace.Query{},
		useSessionPool:       os.Getenv("YDB_GO_SDK_QUERY_SERVICE_USE_SESSION_POOL") != "",
	}
}

// Trace defines trace over table client calls
func (c *Config) Trace() *trace.Query {
	return c.trace
}

// PoolLimit is an upper bound of pooled sessions.
// If PoolLimit is less than or equal to zero then the
// DefaultPoolMaxSize variable is used as a pool limit.
func (c *Config) PoolLimit() int {
	return c.poolLimit
}

// SessionCreateTimeout limits maximum time spent on Create session request
func (c *Config) SessionCreateTimeout() time.Duration {
	return c.sessionCreateTimeout
}

// SessionDeleteTimeout limits maximum time spent on Delete request
//
// If SessionDeleteTimeout is less than or equal to zero then the DefaultSessionDeleteTimeout is used.
func (c *Config) SessionDeleteTimeout() time.Duration {
	return c.sessionDeleteTimeout
}

func (c *Config) UseSessionPool() bool {
	return c.useSessionPool
}
