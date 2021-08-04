package ydbx

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
)

type ClientOption func(client *Client) error

func WithDriver(driver ydb.Driver) ClientOption {
	return func(c *Client) error {
		c.Driver = driver
		return nil
	}
}

func WithCredentials(credentials ydb.Credentials) ClientOption {
	return func(c *Client) error {
		c.DriverConfig.Credentials = credentials
		return nil
	}
}

func WithDriverConfig(config *ydb.DriverConfig) ClientOption {
	return func(c *Client) error {
		c.DriverConfig = config
		return nil
	}
}
