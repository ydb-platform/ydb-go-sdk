package ydb

import (
	"context"
	"crypto/tls"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dial"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"google.golang.org/grpc"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
)

type DB interface {
	grpc.ClientConnInterface

	Table() table.Client
	Scheme() scheme.Client
}

type db struct {
	database string
	options  options
	cluster  cluster.Cluster
	table    *lazyTable
	scheme   *lazyScheme
}

func (c *db) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	return c.cluster.Invoke(ctx, method, args, reply, opts...)
}

func (c *db) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.cluster.NewStream(ctx, desc, method, opts...)
}

func (c *db) Stats(it func(cluster.Endpoint, stats.Stats)) {
	c.cluster.Stats(it)
}

func (c *db) Close() error {
	_ = c.Table().Close(context.Background())
	_ = c.Scheme().Close(context.Background())
	return c.cluster.Close()
}

func (c *db) Table() table.Client {
	return c.table
}

func (c *db) Scheme() scheme.Client {
	return c.scheme
}

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
