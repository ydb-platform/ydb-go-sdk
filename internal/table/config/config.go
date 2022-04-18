package config

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DefaultSessionPoolKeepAliveTimeout     = 500 * time.Millisecond
	DefaultSessionPoolDeleteTimeout        = 500 * time.Millisecond
	DefaultSessionPoolCreateSessionTimeout = 5 * time.Second
	DefaultSessionPoolIdleThreshold        = 5 * time.Minute
	DefaultSessionPoolSizeLimit            = 50
	DefaultKeepAliveMinSize                = 10
	DefaultIdleKeepAliveThreshold          = 2
)

func New(opts ...Option) Config {
	c := defaults()
	for _, o := range opts {
		o(&c)
	}
	return c
}

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

// WithKeepAliveMinSize defines lower bound for sessions in the pool. If there are more sessions open, then
// the excess idle ones will be closed and removed after IdleKeepAliveThreshold is reached for each of them.
// If keepAliveMinSize is less than zero, then no sessions will be preserved
// If keepAliveMinSize is zero, the DefaultKeepAliveMinSize is used
func WithKeepAliveMinSize(keepAliveMinSize int) Option {
	return func(c *Config) {
		if keepAliveMinSize < 0 {
			keepAliveMinSize = 0
		}
		c.keepAliveMinSize = keepAliveMinSize
	}
}

// WithIdleKeepAliveThreshold defines number of keepAlive messages to call before the
// session is removed if it is an excess session (see KeepAliveMinSize)
// This means that session will be deleted after the expiration of lifetime = IdleThreshold * IdleKeepAliveThreshold
// If IdleKeepAliveThreshold is less than zero then it will be treated as infinite and no sessions will
// be removed ever.
// If IdleKeepAliveThreshold is equal to zero, it will be set to DefaultIdleKeepAliveThreshold
func WithIdleKeepAliveThreshold(idleKeepAliveThreshold int) Option {
	return func(c *Config) {
		if idleKeepAliveThreshold > 0 {
			c.idleKeepAliveThreshold = idleKeepAliveThreshold
		}
	}
}

// WithIdleThreshold sets maximum duration between any activity within session.
// If this threshold reached, KeepAlive() method will be called on idle
// session.
//
// If idleThreshold is less than zero then there is no idle limit.
// If idleThreshold is zero, then the DefaultSessionPoolIdleThreshold value
// is used.
func WithIdleThreshold(idleThreshold time.Duration) Option {
	return func(c *Config) {
		if idleThreshold < 0 {
			idleThreshold = 0
		}
		c.idleThreshold = idleThreshold
	}
}

// WithKeepAliveTimeout limits maximum time spent on KeepAlive request
// If keepAliveTimeout is less than or equal to zero then the DefaultSessionPoolKeepAliveTimeout is used.
func WithKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(c *Config) {
		if keepAliveTimeout > 0 {
			c.keepAliveTimeout = keepAliveTimeout
		}
	}
}

// WithCreateSessionTimeout limits maximum time spent on Create session request
// If createSessionTimeout is less than or equal to zero then the
// DefaultSessionPoolCreateSessionTimeout is used.
func WithCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(c *Config) {
		if createSessionTimeout > 0 {
			c.createSessionTimeout = createSessionTimeout
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

// WithTrace appends table trace to early defined traces
func WithTrace(trace trace.Table, opts ...trace.TableComposeOption) Option {
	return func(c *Config) {
		c.trace = c.trace.Compose(trace, opts...)
	}
}

// Config is a configuration of table client
type Config struct {
	config.Common

	sizeLimit              int
	keepAliveMinSize       int
	idleKeepAliveThreshold int
	idleThreshold          time.Duration
	keepAliveTimeout       time.Duration
	createSessionTimeout   time.Duration
	deleteTimeout          time.Duration

	trace trace.Table
}

// Trace defines trace over table client calls
func (c Config) Trace() trace.Table {
	return c.trace
}

// SizeLimit is an upper bound of pooled sessions.
// If SizeLimit is less than or equal to zero then the
// DefaultSessionPoolSizeLimit variable is used as a limit.
func (c Config) SizeLimit() int {
	return c.sizeLimit
}

// KeepAliveMinSize is a lower bound for sessions in the pool. If there are more sessions open, then
// the excess idle ones will be closed and removed after IdleKeepAliveThreshold is reached for each of them.
// If KeepAliveMinSize is less than zero, then no sessions will be preserved
// If KeepAliveMinSize is zero, the DefaultKeepAliveMinSize is used
func (c Config) KeepAliveMinSize() int {
	return c.keepAliveMinSize
}

// IdleKeepAliveThreshold is a number of keepAlive messages to call before the
// session is removed if it is an excess session (see KeepAliveMinSize)
// This means that session will be deleted after the expiration of lifetime = IdleThreshold * IdleKeepAliveThreshold
// If IdleKeepAliveThreshold is less than zero then it will be treated as infinite and no sessions will
// be removed ever.
// If IdleKeepAliveThreshold is equal to zero, it will be set to DefaultIdleKeepAliveThreshold
func (c Config) IdleKeepAliveThreshold() int {
	return c.idleKeepAliveThreshold
}

// IdleThreshold is a maximum duration between any activity within session.
// If this threshold reached, KeepAlive() method will be called on idle
// session.
//
// If IdleThreshold is less than zero then there is no idle limit.
// If IdleThreshold is zero, then the DefaultSessionPoolIdleThreshold value
// is used.
func (c Config) IdleThreshold() time.Duration {
	return c.idleThreshold
}

// KeepAliveTimeout limits maximum time spent on KeepAlive request
// If KeepAliveTimeout is less than or equal to zero then the
// DefaultSessionPoolKeepAliveTimeout is used.
func (c Config) KeepAliveTimeout() time.Duration {
	return c.keepAliveTimeout
}

// CreateSessionTimeout limits maximum time spent on Create session request
// If CreateSessionTimeout is less than or equal to zero then the
// DefaultSessionPoolCreateSessionTimeout is used.
func (c Config) CreateSessionTimeout() time.Duration {
	return c.createSessionTimeout
}

// DeleteTimeout limits maximum time spent on Delete request
// If DeleteTimeout is less than or equal to zero then the
// DefaultSessionPoolDeleteTimeout is used.
func (c Config) DeleteTimeout() time.Duration {
	return c.deleteTimeout
}

func defaults() Config {
	return Config{
		sizeLimit:              DefaultSessionPoolSizeLimit,
		keepAliveMinSize:       DefaultKeepAliveMinSize,
		idleKeepAliveThreshold: DefaultIdleKeepAliveThreshold,
		idleThreshold:          DefaultSessionPoolIdleThreshold,
		keepAliveTimeout:       DefaultSessionPoolKeepAliveTimeout,
		createSessionTimeout:   DefaultSessionPoolCreateSessionTimeout,
		deleteTimeout:          DefaultSessionPoolDeleteTimeout,
	}
}
