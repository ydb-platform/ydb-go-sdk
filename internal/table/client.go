package table

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
)

type Client interface {
	CreateSession(ctx context.Context) (*Session, error)
	Retry(ctx context.Context, retryNoIdempotent bool, op RetryOperation) (err error, issues []error)
	Close(ctx context.Context) error
}

type Config struct {
	// Trace is an optional session lifetime tracing options.
	//Trace sessiontrace.Trace

	// SizeLimit is an upper bound of pooled sessions.
	// If SizeLimit is less than or equal to zero then the
	// DefaultSessionPoolSizeLimit variable is used as a limit.
	SizeLimit int

	// KeepAliveMinSize is a lower bound for sessions in the pool. If there are more sessions open, then
	// the excess idle ones will be closed and removed after IdleKeepAliveThreshold is reached for each of them.
	// If KeepAliveMinSize is less than zero, then no sessions will be preserved
	// If KeepAliveMinSize is zero, the DefaultKeepAliveMinSize is used
	KeepAliveMinSize int

	// IdleKeepAliveThreshold is a number of keepAlive messages to call before the
	// Session is removed if it is an excess session (see KeepAliveMinSize)
	// This means that session will be deleted after the expiration of lifetime = IdleThreshold * IdleKeepAliveThreshold
	// If IdleKeepAliveThreshold is less than zero then it will be treated as infinite and no sessions will
	// be removed ever.
	// If IdleKeepAliveThreshold is equal to zero, it will be set to DefaultIdleKeepAliveThreshold
	IdleKeepAliveThreshold int

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
	IdleThreshold time.Duration

	// KeepAliveTimeout limits maximum time spent on KeepAlive request
	// If KeepAliveTimeout is less than or equal to zero then the
	// DefaultSessionPoolKeepAliveTimeout is used.
	KeepAliveTimeout time.Duration

	// CreateSessionTimeout limits maximum time spent on Create session request
	// If CreateSessionTimeout is less than or equal to zero then the
	// DefaultSessionPoolCreateSessionTimeout is used.
	CreateSessionTimeout time.Duration

	// DeleteTimeout limits maximum time spent on Delete request
	// If DeleteTimeout is less than or equal to zero then the
	// DefaultSessionPoolDeleteTimeout is used.
	DeleteTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		SizeLimit:              DefaultSessionPoolSizeLimit,
		KeepAliveMinSize:       DefaultKeepAliveMinSize,
		IdleKeepAliveThreshold: DefaultIdleKeepAliveThreshold,
		IdleThreshold:          DefaultSessionPoolIdleThreshold,
		KeepAliveTimeout:       DefaultSessionPoolKeepAliveTimeout,
		CreateSessionTimeout:   DefaultSessionPoolCreateSessionTimeout,
		DeleteTimeout:          DefaultSessionPoolDeleteTimeout,
	}
}

func NewClient(db cluster.DB, config Config) Client {
	c := &client{
		cluster: db,
	}
	c.pool = &SessionPool{
		//Trace:                  config.Trace.SessionPoolTrace,
		Builder:                c,
		SizeLimit:              config.SizeLimit,
		KeepAliveMinSize:       config.KeepAliveMinSize,
		IdleKeepAliveThreshold: config.IdleKeepAliveThreshold,
		IdleThreshold:          config.IdleThreshold,
		KeepAliveTimeout:       config.KeepAliveTimeout,
		CreateSessionTimeout:   config.CreateSessionTimeout,
		DeleteTimeout:          config.DeleteTimeout,
	}
	return c
}
