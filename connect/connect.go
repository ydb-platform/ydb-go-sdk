package connect

import (
	"context"
	"crypto/tls"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

// New connects to database and return database connection
func New(ctx context.Context, params ConnectParams, opts ...Option) (c *Connection, err error) {
	c = &Connection{
		table:  newTableWrapper(ctx),
		scheme: newSchemeWrapper(ctx),
	}
	for _, opt := range opts {
		err = opt(ctx, c)
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
	c.cluster, err = (&ydb.Dialer{
		DriverConfig: c.driverConfig,
		TLSConfig:    tlsConfig,
	}).Dial(ctx, params.Endpoint())
	if err != nil {
		return nil, err
	}
	c.table.set(c.cluster)
	c.scheme.set(c.cluster)
	return c, nil
}
