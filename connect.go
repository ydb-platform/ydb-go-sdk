package ydb

import (
	"context"
	"crypto/tls"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dial"
	"time"
)

// New connects to database and return database connection
func New(ctx context.Context, params ConnectParams, opts ...Option) (c *Connection, err error) {
	c = &Connection{
		database: params.Database(),
		table:    &lazyTable{},
		scheme:   &lazyScheme{},
	}
	for _, opt := range opts {
		err = opt(ctx, c)
		if err != nil {
			return nil, err
		}
	}
	var tlsConfig *tls.Config
	if params.UseTLS() {
		tlsConfig = new(tls.Config)
	}
	if c.options.connectTimeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *c.options.connectTimeout)
		defer cancel()
	}
	var grpcConnTTL time.Duration
	if c.options.connectionTTL != nil {
		grpcConnTTL = *c.options.connectionTTL
	}

	c.cluster, err = (&dial.Dialer{
		DriverConfig: &config.Config{
			Database:      params.Database(),
			Credentials:   c.options.credentials,
			ConnectionTTL: grpcConnTTL,
		},
		TLSConfig: tlsConfig,
	}).Dial(ctx, params.Endpoint())
	if err != nil {
		return nil, err
	}
	c.table.set(c.cluster, c.options)
	c.scheme.set(c.cluster, c.options)
	return c, nil
}
