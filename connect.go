package ydb

import (
	"context"
	"crypto/tls"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dial"
	"time"
)

// New connects to database and return database runtime holder
func New(ctx context.Context, params ConnectParams, opts ...Option) (_ DB, err error) {
	c := &db{
		database: params.Database(),
		table:    &lazyTable{},
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
	c.table = newTable(c.cluster, tableConfig(c.options))
	c.scheme = newScheme(c.cluster)
	return c, nil
}
