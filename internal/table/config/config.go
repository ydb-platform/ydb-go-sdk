package config

import (
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DefaultSessionPoolDeleteTimeout        = 500 * time.Millisecond
	DefaultSessionPoolCreateSessionTimeout = 5 * time.Second
	DefaultSessionPoolSizeLimit            = 50
	DefaultSessionPoolIdleThreshold        = 5 * time.Minute

	// Deprecated: table client do not supports background session keep-aliving now.
	// Will be removed after Oct 2024.
	// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
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
// If poolLimit is less than or equal to zero then the
// DefaultSessionPoolSizeLimit variable is used as a limit.
func WithSizeLimit(sizeLimit int) Option {
	return func(c *Config) {
		if sizeLimit > 0 {
			c.poolLimit = sizeLimit
		}
	}
}

// WithSessionPoolSessionUsageLimit set pool session max usage:
// - if argument type is uint64 - WithSessionPoolSessionUsageLimit limits max usage count of pool session
// - if argument type is time.Duration - WithSessionPoolSessionUsageLimit limits max time to live of pool session
func WithSessionPoolSessionUsageLimit[T interface{ uint64 | time.Duration }](limit T) Option {
	return func(c *Config) {
		switch v := any(limit).(type) {
		case uint64:
			c.poolSessionUsageLimit = v
		case time.Duration:
			c.poolSessionUsageTTL = v
		}
	}
}

// WithSessionPoolWarmUpSessions sets the number of sessions to pre-create in the pool at client initialization.
// If poolWarmUpSize is less than or equal to zero, pool warm-up is disabled.
func WithSessionPoolWarmUpSessions(poolWarmUpSize int) Option {
	return func(c *Config) {
		c.poolWarmUpSize = poolWarmUpSize
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
		c.createSessionTimeout = max(createSessionTimeout, 0)
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
func WithTrace(t *trace.Table) Option {
	return func(c *Config) {
		var opts []gtrace.TableComposeOption
		if cb := c.PanicCallback(); cb != nil {
			opts = append(opts, gtrace.WithTablePanicCallback(cb))
		}
		c.trace = gtrace.Compose(c.trace, t, opts...)
	}
}

// WithIgnoreTruncated disables errors on truncated flag
func WithIgnoreTruncated() Option {
	return func(c *Config) {
		c.ignoreTruncated = true
	}
}

// WithMaxRequestMessageSize sets the maximum size of request message in bytes.
func WithMaxRequestMessageSize(maxMessageSize int) Option {
	return func(c *Config) {
		c.maxRequestMessageSize = maxMessageSize
	}
}

// ExecuteDataQueryOverQueryService overrides Execute handle with query service execute with materialized result
func ExecuteDataQueryOverQueryService(b bool) Option {
	return func(c *Config) {
		c.executeDataQueryOverQueryService = b
		if b {
			c.useQuerySession = true
		}
	}
}

// UseQuerySession creates session using query service client
func UseQuerySession(b bool) Option {
	return func(c *Config) {
		c.useQuerySession = b
	}
}

func WithDisableSessionBalancer() Option {
	return func(c *Config) {
		c.SetDisableSessionBalancer()
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

	poolLimit             int
	poolSessionUsageLimit uint64
	poolSessionUsageTTL   time.Duration

	createSessionTimeout time.Duration
	deleteTimeout        time.Duration
	idleThreshold        time.Duration
	poolWarmUpSize       int

	ignoreTruncated                  bool
	useQuerySession                  bool
	executeDataQueryOverQueryService bool

	maxRequestMessageSize int

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
	return c.poolLimit
}

func (c *Config) SessionUsageLimit() uint64 {
	return c.poolSessionUsageLimit
}

func (c *Config) SessionUsageTTL() time.Duration {
	return c.poolSessionUsageTTL
}

// PoolWarmUpSize is the number of sessions to pre-create in the pool at client initialization.
// If PoolWarmUpSize is less than or equal to zero, pool warm-up is disabled.
func (c *Config) PoolWarmUpSize() int {
	if c.poolWarmUpSize <= 0 {
		return 0
	}

	return c.poolWarmUpSize
}

// IgnoreTruncated specifies behavior on truncated flag
func (c *Config) IgnoreTruncated() bool {
	return c.ignoreTruncated
}

// UseQuerySession specifies behavior on create/delete session
func (c *Config) UseQuerySession() bool {
	return c.useQuerySession
}

// ExecuteDataQueryOverQueryService specifies behavior on execute handle
func (c *Config) ExecuteDataQueryOverQueryService() bool {
	return c.executeDataQueryOverQueryService
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
// Deprecated: table client do not supports background session keep-aliving now.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
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

// MaxRequestMessageSize returns the maximum size in bytes for a single request message.
//
// If the value is exceeded, the request will be split into several parts.
func (c *Config) MaxRequestMessageSize() int {
	return c.maxRequestMessageSize
}

func defaults() *Config {
	return &Config{
		poolLimit:            DefaultSessionPoolSizeLimit,
		poolWarmUpSize:       0,
		createSessionTimeout: DefaultSessionPoolCreateSessionTimeout,
		deleteTimeout:        DefaultSessionPoolDeleteTimeout,
		idleThreshold:        DefaultSessionPoolIdleThreshold,
		clock:                clockwork.NewRealClock(),
		trace:                &trace.Table{},
	}
}
