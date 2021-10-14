package ydb

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dial"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type DB cluster.Cluster

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
	options      []config.Option
	cluster      cluster.Cluster
	table        lazyTable
	scheme       lazyScheme
	coordination lazyCoordination
	ratelimiter  lazyRatelimiter
	discovery    lazyDiscovery
}

func (db *db) Discovery() discovery.Client {
	return &db.discovery
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

func (db *db) Close(ctx context.Context) error {
	_ = db.Table().Close(ctx)
	_ = db.Scheme().Close(ctx)
	_ = db.Coordination().Close(ctx)
	return db.cluster.Close(ctx)
}

func (db *db) Table() table.Client {
	return &db.table
}

func (db *db) Scheme() scheme.Client {
	return &db.scheme
}

func (db *db) Coordination() coordination.Client {
	return &db.coordination
}

func (db *db) RateLimiter() ratelimiter.Client {
	return &db.ratelimiter
}

// New connects to name and return name runtime holder
func New(ctx context.Context, opts ...Option) (_ Connection, err error) {
	db := &db{}
	for _, opt := range opts {
		err = opt(ctx, db)
		if err != nil {
			return nil, err
		}
	}
	c := config.New(db.options...)
	db.name = c.Database()
	if tlsConfig := c.TLSConfig(); tlsConfig != nil && tlsConfig.RootCAs == nil {
		var certPool *x509.CertPool
		certPool, err = x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("loading system certificates pool failed: %v", err)
		}
		if caFile, has := os.LookupEnv("YDB_SSL_ROOT_CERTIFICATES_FILE"); has {
			// ignore any errors on load certificates
			if err = credentials.AppendCertsFromFile(certPool, caFile); err != nil {
				return nil, fmt.Errorf("loading certificates from file '%s' by Env['YDB_SSL_ROOT_CERTIFICATES_FILE'] failed: %v", caFile, err)
			}
		}
		tlsConfig.RootCAs = certPool
	}
	if err != nil {
		return nil, err
	}
	db.cluster, err = dial.Dial(ctx, c)
	if err != nil {
		return nil, err
	}
	db.table.db = db.cluster
	db.coordination.db = db.cluster
	db.ratelimiter.db = db.cluster
	db.discovery.db = db.cluster
	db.scheme.db = db
	db.discovery.trace = c.Trace()
	return db, nil
}
