package ydb

import (
	"context"
	"os"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dial"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type DB interface {
	cluster.Cluster

	// Name returns database name
	Name() string

	// Secure returns true if database connection is secure
	Secure() bool
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
	config       config.Config
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
	return db.config.Database()
}

func (db *db) Secure() bool {
	return db.config.Secure()
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
	opts = append([]Option{WithConnectionString(os.Getenv("YDB_CONNECTION_STRING"))}, opts...)
	for _, opt := range opts {
		err = opt(ctx, db)
		if err != nil {
			return nil, err
		}
	}
	db.config = config.New(db.options...)
	if err != nil {
		return nil, err
	}
	db.cluster, err = dial.Dial(ctx, db.config)
	if err != nil {
		return nil, err
	}
	db.table.db = db
	db.coordination.db = db
	db.ratelimiter.db = db
	db.discovery.db = db
	db.scheme.db = db
	db.discovery.trace = db.config.Trace()
	return db, nil
}
