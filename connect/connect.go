package connect

import (
	"context"
	"crypto/tls"
	"github.com/YandexDatabase/ydb-go-sdk/v2/scheme"
	"github.com/YandexDatabase/ydb-go-sdk/v2/table"

	"github.com/YandexDatabase/ydb-go-sdk/v2"
	"github.com/YandexDatabase/ydb-go-sdk/v2/auth"
)

// New connects to database and return database connection
func New(ctx context.Context, params ConnectParams, opts ...ConnectOption) (c *Connection, err error) {
	c = &Connection{
		table:  newTableWrapper(ctx),
		scheme: newSchemeWrapper(ctx),
	}
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
	c.table.client = table.NewClient(c.driver)
	c.table.sessionPool.Builder = c.table.client
	c.scheme.client = scheme.NewClient(c.driver)
	return c, nil
}
