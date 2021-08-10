package connect

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/auth"
	"context"
	"crypto/tls"
)

// New connects to database and return database connection
func New(ctx context.Context, params ConnectParams, opts ...ConnectOption) (c *Connection, err error) {
	c = &Connection{}
	for _, opt := range opts {
		err := opt(c)
		if err != nil {
			return nil, err
		}
	}
	if c.driverConfig == nil {
		c.driverConfig = &ydb.DriverConfig{}
	}
	c.driverConfig.Database = params.Database()
	if c.driverConfig.Credentials == nil {
		c.driverConfig.Credentials, err = auth.FromEnviron(ctx)
		if err != nil {
			return nil, err
		}
	}
	var tlsConfig *tls.Config
	if params.UseTLS() {
		tlsConfig = new(tls.Config)
	}
	c.driver, err = (&ydb.Dialer{
		DriverConfig: c.driverConfig,
		TLSConfig:    tlsConfig,
	}).Dial(ctx, params.Endpoint())
	if err != nil {
		return nil, err
	}
	c.table = newTableWrapper(ctx, c.driver)
	c.scheme = newSchemeWrapper(ctx, c.driver)
	return c, nil
}
