package config

import (
	"time"

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

type Config interface {
	// Trace is an optional session lifetime tracing options.
	Trace() trace.Table

	// SizeLimit is an upper bound of pooled sessions.
	// If SizeLimit is less than or equal to zero then the
	// DefaultSessionPoolSizeLimit variable is used as a limit.
	SizeLimit() int

	// KeepAliveMinSize is a lower bound for sessions in the pool. If there are more sessions open, then
	// the excess idle ones will be closed and removed after IdleKeepAliveThreshold is reached for each of them.
	// If KeepAliveMinSize is less than zero, then no sessions will be preserved
	// If KeepAliveMinSize is zero, the DefaultKeepAliveMinSize is used
	KeepAliveMinSize() int

	// IdleKeepAliveThreshold is a number of keepAlive messages to call before the
	// session is removed if it is an excess session (see KeepAliveMinSize)
	// This means that session will be deleted after the expiration of lifetime = IdleThreshold * IdleKeepAliveThreshold
	// If IdleKeepAliveThreshold is less than zero then it will be treated as infinite and no sessions will
	// be removed ever.
	// If IdleKeepAliveThreshold is equal to zero, it will be set to DefaultIdleKeepAliveThreshold
	IdleKeepAliveThreshold() int

	// IdleLimit is an upper bound of pooled sessions without any activity
	// within.
	// IdleLimit int

	// IdleThreshold is a maximum duration between any activity within session.
	// If this threshold reached, KeepAlive() method will be called on idle
	// session.
	//
	// If IdleThreshold is less than zero then there is no idle limit.
	// If IdleThreshold is zero, then the DefaultSessionPoolIdleThreshold value
	// is used.
	IdleThreshold() time.Duration

	// KeepAliveTimeout limits maximum time spent on KeepAlive request
	// If KeepAliveTimeout is less than or equal to zero then the
	// DefaultSessionPoolKeepAliveTimeout is used.
	KeepAliveTimeout() time.Duration

	// CreateSessionTimeout limits maximum time spent on Create session request
	// If CreateSessionTimeout is less than or equal to zero then the
	// DefaultSessionPoolCreateSessionTimeout is used.
	CreateSessionTimeout() time.Duration

	// DeleteTimeout limits maximum time spent on Delete request
	// If DeleteTimeout is less than or equal to zero then the
	// DefaultSessionPoolDeleteTimeout is used.
	DeleteTimeout() time.Duration
}

func New(opts ...Option) Config {
	c := defaults()
	for _, o := range opts {
		o(c)
	}
	return c
}

type Option func(*config)

func WithSizeLimit(sizeLimit int) Option {
	return func(c *config) {
		if sizeLimit > 0 {
			c.sizeLimit = sizeLimit
		}
	}
}

func WithKeepAliveMinSize(keepAliveMinSize int) Option {
	return func(c *config) {
		if keepAliveMinSize < 0 {
			keepAliveMinSize = 0
		}
		c.keepAliveMinSize = keepAliveMinSize
	}
}

func WithIdleKeepAliveThreshold(idleKeepAliveThreshold int) Option {
	return func(c *config) {
		if idleKeepAliveThreshold > 0 {
			c.idleKeepAliveThreshold = idleKeepAliveThreshold
		}
	}
}

func WithIdleThreshold(idleThreshold time.Duration) Option {
	return func(c *config) {
		if idleThreshold < 0 {
			idleThreshold = 0
		}
		c.idleThreshold = idleThreshold
	}
}

func WithKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(c *config) {
		if keepAliveTimeout > 0 {
			c.keepAliveTimeout = keepAliveTimeout
		}
	}
}

func WithCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(c *config) {
		if createSessionTimeout > 0 {
			c.createSessionTimeout = createSessionTimeout
		}
	}
}

func WithDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(c *config) {
		if deleteTimeout > 0 {
			c.deleteTimeout = deleteTimeout
		}
	}
}

func WithTrace(trace trace.Table) Option {
	return func(c *config) {
		c.trace = c.trace.Compose(trace)
	}
}

type config struct {
	sizeLimit              int
	keepAliveMinSize       int
	idleKeepAliveThreshold int
	idleThreshold          time.Duration
	keepAliveTimeout       time.Duration
	createSessionTimeout   time.Duration
	deleteTimeout          time.Duration
	trace                  trace.Table
}

func (c *config) Trace() trace.Table {
	return c.trace
}

func (c *config) SizeLimit() int {
	return c.sizeLimit
}

func (c *config) KeepAliveMinSize() int {
	return c.keepAliveMinSize
}

func (c *config) IdleKeepAliveThreshold() int {
	return c.idleKeepAliveThreshold
}

func (c *config) IdleThreshold() time.Duration {
	return c.idleThreshold
}

func (c *config) KeepAliveTimeout() time.Duration {
	return c.keepAliveTimeout
}

func (c *config) CreateSessionTimeout() time.Duration {
	return c.createSessionTimeout
}

func (c *config) DeleteTimeout() time.Duration {
	return c.deleteTimeout
}

func defaults() *config {
	return &config{
		sizeLimit:              DefaultSessionPoolSizeLimit,
		keepAliveMinSize:       DefaultKeepAliveMinSize,
		idleKeepAliveThreshold: DefaultIdleKeepAliveThreshold,
		idleThreshold:          DefaultSessionPoolIdleThreshold,
		keepAliveTimeout:       DefaultSessionPoolKeepAliveTimeout,
		createSessionTimeout:   DefaultSessionPoolCreateSessionTimeout,
		deleteTimeout:          DefaultSessionPoolDeleteTimeout,
	}
}
