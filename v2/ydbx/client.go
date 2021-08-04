package ydbx

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/auth"
	"context"
	"crypto/tls"
	"crypto/x509"
)

type Client struct {
	DriverConfig *ydb.DriverConfig
	Driver       ydb.Driver
	credentials  ydb.Credentials
	table        *tableWrapper
}

func (c *Client) Close() {
	_ = c.table.Pool().Close(context.Background())
	_ = c.Driver.Close()
}

func (c *Client) Table() *tableWrapper {
	return c.table
}

// NewClient creates a Client to a database
func NewClient(ctx context.Context, params ConnectParams, opts ...ClientOption) (c *Client, err error) {
	c = &Client{}
	for _, opt := range opts {
		err := opt(c)
		if err != nil {
			return nil, err
		}
	}
	if c.Driver == nil {
		if c.DriverConfig == nil {
			c.DriverConfig = &ydb.DriverConfig{}
		}
		c.DriverConfig.Database = params.Database()
		if c.DriverConfig.Credentials == nil {
			c.DriverConfig.Credentials, err = auth.FromEnviron(ctx)
			if err != nil {
				return nil, err
			}
		}
		var tlsConfig *tls.Config
		if params.UseTLS() {
			roots, err := x509.SystemCertPool()
			if err != nil {
				return nil, err
			}
			tlsConfig = &tls.Config{
				RootCAs: roots,
			}
		}
		c.Driver, err = (&ydb.Dialer{
			DriverConfig: c.DriverConfig,
			TLSConfig:    tlsConfig,
		}).Dial(ctx, params.Endpoint())
		if err != nil {
			return nil, err
		}
	}
	c.table = newTableWrapper(c.Driver)
	return c, nil
}
