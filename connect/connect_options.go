package connect

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"time"
)

type ConnectOption func(client *Connection) error

func WithCredentials(credentials ydb.Credentials) ConnectOption {
	return func(c *Connection) error {
		if c.driverConfig == nil {
			c.driverConfig = &ydb.DriverConfig{}
		}
		c.driverConfig.Credentials = credentials
		return nil
	}
}

func WithDriverConfig(config *ydb.DriverConfig) ConnectOption {
	return func(c *Connection) error {
		c.driverConfig = config
		return nil
	}
}

func withDriver(driver ydb.Driver) ConnectOption {
	return func(c *Connection) error {
		c.table.client.Driver = driver
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

func WithSessionPoolKeepAliveBatchSize(keepAliveBatchSize int) ConnectOption {
	return func(c *Connection) error {
		c.table.sessionPool.KeepAliveBatchSize = keepAliveBatchSize
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
