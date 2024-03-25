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

	// Deprecated: table client do not supports background session keep-aliving now
	DefaultKeepAliveMinSize = 10

	// Deprecated: table client do not supports background session keep-aliving now
	DefaultIdleKeepAliveThreshold = 2

	// Deprecated: table client do not supports background session keep-aliving now
	DefaultSessionPoolKeepAliveTimeout = 500 * time.Millisecond
)

func New(opts ...Option) *Config {
	c := defaults()
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
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
//
// Deprecated: table client do not supports background session keep-aliving now
func WithKeepAliveMinSize(keepAliveMinSize int) Option {
	return func(c *Config) {}
}

// WithIdleKeepAliveThreshold defines number of keepAlive messages to call before the
// session is removed if it is an excess session (see KeepAliveMinSize)
// This means that session will be deleted after the expiration of lifetime = IdleThreshold * IdleKeepAliveThreshold
// If IdleKeepAliveThreshold is less than zero then it will be treated as infinite and no sessions will
// be removed ever.
// If IdleKeepAliveThreshold is equal to zero, it will be set to DefaultIdleKeepAliveThreshold
//
// Deprecated: table client do not support background session keep-aliving now
func WithIdleKeepAliveThreshold(idleKeepAliveThreshold int) Option {
	return func(c *Config) {}
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

// WithKeepAliveTimeout limits maximum time spent on KeepAlive request
// If keepAliveTimeout is less than or equal to zero then the DefaultSessionPoolKeepAliveTimeout is used.
//
// Deprecated: table client do not support background session keep-aliving now
func WithKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(c *Config) {}
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

// WithTrace appends table trace to early defined traces
func WithTrace(trace *trace.Table, opts ...trace.TableComposeOption) Option {
	return func(c *Config) {
		c.trace = c.trace.Compose(trace, opts...)
	}
}

// WithIgnoreTruncated disables errors on truncated flag
func WithIgnoreTruncated() Option {
	return func(c *Config) {
		c.ignoreTruncated = true
	}
}

// WithClock replaces default clock
func WithClock(clock clockwork.Clock) Option {
	return func(c *Config) {
		c.clock = clock
	}
}

// Config is a configuration of table client
type Config struct {
	config.Common

	sizeLimit int

	createSessionTimeout time.Duration
	deleteTimeout        time.Duration
	idleThreshold        time.Duration

	ignoreTruncated bool

	trace *trace.Table

	clock clockwork.Clock
}

// Trace defines trace over table client calls
func (c *Config) Trace() *trace.Table {
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

// KeepAliveMinSize is a lower bound for sessions in the pool. If there are more sessions open, then
// the excess idle ones will be closed and removed after IdleKeepAliveThreshold is reached for each of them.
// If KeepAliveMinSize is less than zero, then no sessions will be preserved
// If KeepAliveMinSize is zero, the DefaultKeepAliveMinSize is used
//
// Deprecated: table client do not support background session keep-aliving now
func (c *Config) KeepAliveMinSize() int {
	return DefaultKeepAliveMinSize
}

// IgnoreTruncated specifies behavior on truncated flag
func (c *Config) IgnoreTruncated() bool {
	return c.ignoreTruncated
}

// IdleKeepAliveThreshold is a number of keepAlive messages to call before the
// session is removed if it is an excess session (see KeepAliveMinSize)
// This means that session will be deleted after the expiration of lifetime = IdleThreshold * IdleKeepAliveThreshold
// If IdleKeepAliveThreshold is less than zero then it will be treated as infinite and no sessions will
// be removed ever.
// If IdleKeepAliveThreshold is equal to zero, it will be set to DefaultIdleKeepAliveThreshold
//
// Deprecated: table client do not support background session keep-aliving now
func (c *Config) IdleKeepAliveThreshold() int {
	return DefaultIdleKeepAliveThreshold
}

// IdleThreshold is a maximum duration between any activity within session.
// If this threshold reached, idle session will be closed
//
// If IdleThreshold is less than zero then there is no idle limit.
// If IdleThreshold is zero, then the DefaultSessionPoolIdleThreshold value is used.
func (c *Config) IdleThreshold() time.Duration {
	return c.idleThreshold
}

// KeepAliveTimeout limits maximum time spent on KeepAlive request
// If KeepAliveTimeout is less than or equal to zero then the DefaultSessionPoolKeepAliveTimeout is used.
//
// Deprecated: table client do not support background session keep-aliving now
func (c *Config) KeepAliveTimeout() time.Duration {
	return DefaultSessionPoolKeepAliveTimeout
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

func defaults() *Config {
	return &Config{
		sizeLimit:            DefaultSessionPoolSizeLimit,
		createSessionTimeout: DefaultSessionPoolCreateSessionTimeout,
		deleteTimeout:        DefaultSessionPoolDeleteTimeout,
		idleThreshold:        DefaultSessionPoolIdleThreshold,
		clock:                clockwork.NewRealClock(),
		trace:                &trace.Table{},
	}
}
