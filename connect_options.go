package ydb

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/sessiontrace"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	icredentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(ctx context.Context, client *db) error

type options struct {
	connectTimeout                       *time.Duration
	driverTrace                          *trace.DriverTrace
	driverConfig                         *config.Config
	credentials                          icredentials.Credentials
	connectionTTL                        *time.Duration
	discoveryInterval                    *time.Duration
	tableSessionPoolTrace                *sessiontrace.SessionPoolTrace
	tableSessionPoolSizeLimit            *int
	tableSessionPoolKeepAliveMinSize     *int
	tableSessionPoolIdleThreshold        *time.Duration
	tableSessionPoolKeepAliveTimeout     *time.Duration
	tableSessionPoolCreateSessionTimeout *time.Duration
	tableSessionPoolDeleteTimeout        *time.Duration
	tableClientTrace                     *sessiontrace.Trace
}

func WithAccessTokenCredentials(accessToken string) Option {
	return WithCredentials(
		credentials.NewAuthTokenCredentials(accessToken, "connect.WithAccessTokenCredentials(accessToken)"), // hide access token for logs
	)
}

func WithAnonymousCredentials() Option {
	return WithCredentials(
		credentials.NewAnonymousCredentials("connect.WithAnonymousCredentials()"),
	)
}

func WithCreateCredentialsFunc(createCredentials func(ctx context.Context) (icredentials.Credentials, error)) Option {
	return func(ctx context.Context, c *db) error {
		credentials, err := createCredentials(ctx)
		if err != nil {
			return err
		}
		c.options.credentials = credentials
		return nil
	}
}

func WithCredentials(c icredentials.Credentials) Option {
	return WithCreateCredentialsFunc(func(context.Context) (icredentials.Credentials, error) {
		return c, nil
	})
}

func WithDriverConfig(config *config.Config) Option {
	return func(ctx context.Context, c *db) error {
		c.options.driverConfig = config
		return nil
	}
}

func WithConnectionTTL(ttl time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.connectionTTL = &ttl
		return nil
	}
}

func WithDiscoveryInterval(discoveryInterval time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.discoveryInterval = &discoveryInterval
		return nil
	}
}

func WithSessionPoolSizeLimit(sizeLimit int) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolSizeLimit = &sizeLimit
		return nil
	}
}

func WithSessionPoolKeepAliveMinSize(keepAliveMinSize int) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolKeepAliveMinSize = &keepAliveMinSize
		return nil
	}
}

func WithSessionPoolIdleThreshold(idleThreshold time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolIdleThreshold = &idleThreshold
		return nil
	}
}

func WithSessionPoolKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolKeepAliveTimeout = &keepAliveTimeout
		return nil
	}
}

func WithSessionPoolCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolCreateSessionTimeout = &createSessionTimeout
		return nil
	}
}

func WithSessionPoolDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolDeleteTimeout = &deleteTimeout
		return nil
	}
}

// WithDriverTrace returns context which has associated DriverTrace with it.
func WithDriverTrace(trace trace.DriverTrace) Option {
	return func(ctx context.Context, c *db) error {
		c.options.driverTrace = &trace
		return nil
	}
}

// WithTableClientTrace returns context which has associated DriverTrace with it.
func WithTableClientTrace(trace sessiontrace.Trace) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableClientTrace = &trace
		return nil
	}
}

// WithTableSessionPoolTrace returns context which has associated DriverTrace with it.
func WithTableSessionPoolTrace(trace sessiontrace.SessionPoolTrace) Option {
	return func(ctx context.Context, c *db) error {
		c.options.tableSessionPoolTrace = &trace
		return nil
	}
}

func WithConnectTimeout(connectTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.connectTimeout = &connectTimeout
		return nil
	}
}
