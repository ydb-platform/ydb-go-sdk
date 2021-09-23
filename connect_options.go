package ydb

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	icredentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(ctx context.Context, client *db) error

type options struct {
	connectTimeout                       *time.Duration
	traceDriver                          *trace.Driver
	traceTable                           *trace.Table
	driverConfig                         *config.Config
	credentials                          icredentials.Credentials
	discoveryInterval                    *time.Duration
	tableSessionPoolSizeLimit            *int
	tableSessionPoolKeepAliveMinSize     *int
	tableSessionPoolIdleThreshold        *time.Duration
	tableSessionPoolKeepAliveTimeout     *time.Duration
	tableSessionPoolCreateSessionTimeout *time.Duration
	tableSessionPoolDeleteTimeout        *time.Duration
}

func WithAccessTokenCredentials(accessToken string) Option {
	return WithCredentials(
		credentials.NewAuthTokenCredentials(accessToken, "connect.WithAccessTokenCredentials(accessToken)"), // hide access token for logs
	)
}

func NewAuthTokenCredentials(accessToken string) credentials.Credentials {
	return credentials.NewAuthTokenCredentials(accessToken, "connect.NewAuthTokenCredentials(accessToken)") // hide access token for logs
}

func WithAnonymousCredentials() Option {
	return WithCredentials(
		credentials.NewAnonymousCredentials("connect.WithAnonymousCredentials()"),
	)
}

func NewAnonymousCredentials() credentials.Credentials {
	return credentials.NewAnonymousCredentials("connect.NewAnonymousCredentials()")
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

func WithGrpcConnectionPolicy(policy *config.GrpcConnectionPolicy) Option {
	return func(ctx context.Context, c *db) error {
		c.options.driverConfig.GrpcConnectionPolicy = policy
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

// WithTraceDriver returns deadline which has associated Driver with it.
func WithTraceDriver(trace trace.Driver) Option {
	return func(ctx context.Context, c *db) error {
		c.options.traceDriver = &trace
		return nil
	}
}

// WithTraceTable returns deadline which has associated Driver with it.
func WithTraceTable(trace trace.Table) Option {
	return func(ctx context.Context, c *db) error {
		c.options.traceTable = &trace
		return nil
	}
}

func WithConnectTimeout(connectTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.options.connectTimeout = &connectTimeout
		return nil
	}
}
