package connect

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
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
