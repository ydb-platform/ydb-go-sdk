package connect

import (
	"time"

	"github.com/YandexDatabase/ydb-go-sdk/v3"
)

type ConnectOption func(client *Connection) error

func WithAccessTokenCredentials(accessToken string) ConnectOption {
	return WithCredentials(
		ydb.NewAuthTokenCredentials(accessToken, "connect.WithAccessTokenCredentials(accessToken)"), // hide access token for logs
	)
}

func WithAnonymousCredentials() ConnectOption {
	return WithCredentials(
		ydb.NewAnonymousCredentials("connect.WithAnonymousCredentials()"),
	)
}

func WithCreateCredentialsFunc(createCredentials func() (ydb.Credentials, error)) ConnectOption {
	return func(c *Connection) error {
		if c.driverConfig == nil {
			c.driverConfig = &ydb.DriverConfig{}
		}
		credentials, err := createCredentials()
		if err != nil {
			return err
		}
		c.driverConfig.Credentials = credentials
		return nil
	}
}

func WithCredentials(credentials ydb.Credentials) ConnectOption {
	return WithCreateCredentialsFunc(func() (ydb.Credentials, error) {
		return credentials, nil
	})
}

func WithDriverConfig(config *ydb.DriverConfig) ConnectOption {
	return func(c *Connection) error {
		c.driverConfig = config
		return nil
	}
}

func WithDiscoveryInterval(discoveryInterval time.Duration) ConnectOption {
	return func(c *Connection) error {
		if c.driverConfig == nil {
			c.driverConfig = &ydb.DriverConfig{}
		}
		c.driverConfig.DiscoveryInterval = discoveryInterval
		return nil
	}
}

func WithSessionPoolSizeLimit(sizeLimit int) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.SizeLimit = sizeLimit
		return nil
	}
}

func WithSessionPoolKeepAliveMinSize(keepAliveMinSize int) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.KeepAliveMinSize = keepAliveMinSize
		return nil
	}
}

func WithSessionPoolIdleThreshold(idleThreshold time.Duration) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.IdleThreshold = idleThreshold
		return nil
	}
}

func WithSessionPoolBusyCheckInterval(busyCheckInterval time.Duration) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.BusyCheckInterval = busyCheckInterval
		return nil
	}
}

func WithSessionPoolKeepAliveTimeout(keepAliveTimeout time.Duration) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.KeepAliveTimeout = keepAliveTimeout
		return nil
	}
}

func WithSessionPoolCreateSessionTimeout(createSessionTimeout time.Duration) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.CreateSessionTimeout = createSessionTimeout
		return nil
	}
}

func WithSessionPoolDeleteTimeout(deleteTimeout time.Duration) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.DeleteTimeout = deleteTimeout
		return nil
	}
}
