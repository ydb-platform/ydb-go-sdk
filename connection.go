package ydb

import (
	"context"
	"crypto/tls"
	"crypto/x509"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dial"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type DB interface {
	cluster.DB

	// Stats return cluster stats
	Stats() map[cluster.Endpoint]stats.Stats

	// Close clears resources and close all connections to YDB
	Close() error
}

type Connection interface {
	DB

	Table() table.Client
	Scheme() scheme.Client
	Coordination() coordination.Client
	RateLimiter() ratelimiter.Client
	Discovery() discovery.Client
}

type db struct {
	name         string
	options      options
	cluster      cluster.Cluster
	table        *lazyTable
	scheme       *lazyScheme
	coordination *lazyCoordination
	ratelimiter  *lazyRatelimiter
	discovery    *lazyDiscovery
}

func (db *db) Discovery() discovery.Client {
	return db.discovery
}

func (db *db) Name() string {
	return db.name
}

func (db *db) Secure() bool {
	return db.cluster.Secure()
}

func (db *db) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	return db.cluster.Invoke(ctx, method, args, reply, opts...)
}

func (db *db) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return db.cluster.NewStream(ctx, desc, method, opts...)
}

func (db *db) Stats() map[cluster.Endpoint]stats.Stats {
	return db.cluster.Stats()
}

func (db *db) Close() error {
	_ = db.Table().Close(context.Background())
	_ = db.Scheme().Close(context.Background())
	_ = db.Coordination().Close(context.Background())
	return db.cluster.Close()
}

func (db *db) Table() table.Client {
	return db.table
}

func (db *db) Scheme() scheme.Client {
	return db.scheme
}

func (db *db) Coordination() coordination.Client {
	return db.coordination
}

func (db *db) RateLimiter() ratelimiter.Client {
	return db.ratelimiter
}

// New connects to name and return name runtime holder
func New(ctx context.Context, params ConnectParams, opts ...Option) (_ Connection, err error) {
	db := &db{
		name: params.Database(),
	}
	for _, opt := range opts {
		err = opt(ctx, db)
		if err != nil {
			return nil, err
		}
	}
	var tlsConfig *tls.Config
	if params.UseTLS() {
		tlsConfig = &tls.Config{}
		if db.options.certPool != nil {
			tlsConfig.RootCAs = db.options.certPool
		} else {
			tlsConfig.RootCAs, err = x509.SystemCertPool()
			if err != nil {
				return nil, err
			}
		}
	}
	if db.options.connectTimeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *db.options.connectTimeout)
		defer cancel()
	}
	db.cluster, err = (&dial.Dialer{
		DriverConfig: config.New(
			func(c *config.Config) {
				c.Database = params.Database()
			},
			func(c *config.Config) {
				c.Credentials = db.options.credentials
			},
		),
		TLSConfig: tlsConfig,
	}).Dial(ctx, params.Endpoint())
	if err != nil {
		return nil, err
	}
	db.table = newTable(db.cluster, tableConfig(db.options))
	db.scheme = newScheme(db)
	db.coordination = newCoordination(db.cluster)
	db.ratelimiter = newRatelimiter(db.cluster)
	db.discovery = newDiscovery(db.cluster)
	return db, nil
}
