package connect

import (
	"context"
	"crypto/tls"
	"github.com/YandexDatabase/ydb-go-sdk/v3"
)

// New connects to database and return database connection
func New(ctx context.Context, params ConnectParams, opts ...ConnectOption) (c *Connection, err error) {
	c = &Connection{
		table:  newTableWrapper(ctx),
		scheme: newSchemeWrapper(ctx),
	}
	for _, opt := range opts {
		err = opt(c)
		if err != nil {
			return nil, err
		}
	}
	if c.driverConfig == nil {
		c.driverConfig = &ydb.DriverConfig{}
	}
	c.driverConfig.Database = params.Database()
	if c.driverConfig.Credentials == nil {
		return nil, ydb.ErrCredentialsNoCredentials
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
	c.table.set(c.driver)
	c.scheme.set(c.driver)
	return c, nil
}
