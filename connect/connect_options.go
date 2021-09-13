package connect

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type Option func(ctx context.Context, client *Connection) error

func WithAccessTokenCredentials(accessToken string) Option {
	return WithCredentials(
		ydb.NewAuthTokenCredentials(accessToken, "connect.WithAccessTokenCredentials(accessToken)"), // hide access token for logs
	)
}

func WithAnonymousCredentials() Option {
	return WithCredentials(
		ydb.NewAnonymousCredentials("connect.WithAnonymousCredentials()"),
	)
}

func WithCreateCredentialsFunc(createCredentials func(ctx context.Context) (ydb.Credentials, error)) Option {
	return func(ctx context.Context, c *Connection) error {
		if c.driverConfig == nil {
			c.driverConfig = &ydb.DriverConfig{}
		}
		credentials, err := createCredentials(ctx)
		if err != nil {
			return err
		}
		c.driverConfig.Credentials = credentials
		return nil
	}
}

func WithCredentials(credentials ydb.Credentials) Option {
	return WithCreateCredentialsFunc(func(context.Context) (ydb.Credentials, error) {
		return credentials, nil
	})
}

func WithDriverConfig(config *ydb.DriverConfig) Option {
	return func(ctx context.Context, c *Connection) error {
		c.driverConfig = config
		return nil
	}
}

func WithDiscoveryInterval(discoveryInterval time.Duration) Option {
	return func(ctx context.Context, c *Connection) error {
		if c.driverConfig == nil {
			c.driverConfig = &ydb.DriverConfig{}
		}
		c.driverConfig.DiscoveryInterval = discoveryInterval
		return nil
	}
}

func WithSessionPoolSizeLimit(sizeLimit int) Option {
	return func(ctx context.Context, c *Connection) error {
		c.table.sessionPool.SizeLimit = sizeLimit
		return nil
	}
}

func WithSessionPoolKeepAliveMinSize(keepAliveMinSize int) Option {
	return func(ctx context.Context, c *Connection) error {
		c.table.sessionPool.KeepAliveMinSize = keepAliveMinSize
		return nil
	}
}

func WithSessionPoolIdleThreshold(idleThreshold time.Duration) Option {
	return func(ctx context.Context, c *Connection) error {
		c.table.sessionPool.IdleThreshold = idleThreshold
		return nil
	}
}

func WithSessionPoolKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(ctx context.Context, c *Connection) error {
		c.table.sessionPool.KeepAliveTimeout = keepAliveTimeout
		return nil
	}
}

func WithSessionPoolCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(ctx context.Context, c *Connection) error {
		c.table.sessionPool.CreateSessionTimeout = createSessionTimeout
		return nil
	}
}

func WithSessionPoolDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(ctx context.Context, c *Connection) error {
		c.table.sessionPool.DeleteTimeout = deleteTimeout
		return nil
	}
}
