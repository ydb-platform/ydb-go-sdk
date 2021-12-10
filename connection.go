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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type DB interface {
	cluster.Cluster

	// Endpoint returns initial endpoint
	Endpoint() string

	// Name returns database name
	Name() string

	// Secure returns true if database connection is secure
	Secure() bool
}

type Connection interface {
	DB

	// Table returns table client with options from Connection instance.
	// Options provide options replacement for requested table client
	// such as endpoint, database, secure connection flag and credentials
	// Options replacement feature not implements now
	Table(opts ...Option) table.Client

	// Scheme returns scheme client with options from Connection instance.
	// Options provide options replacement for requested scheme client
	// such as endpoint, database, secure connection flag and credentials
	// Options replacement feature not implements now
	Scheme(opts ...Option) scheme.Client

	// Coordination returns coordination client with options from Connection instance.
	// Options provide options replacement for requested coordination client
	// such as endpoint, database, secure connection flag and credentials
	// Options replacement feature not implements now
	Coordination(opts ...Option) coordination.Client

	// RateLimiter returns rate limiter client with options from Connection instance.
	// Options provide options replacement for requested rate limiter client
	// such as endpoint, database, secure connection flag and credentials
	// Options replacement feature not implements now
	RateLimiter(opts ...Option) ratelimiter.Client

	// Discovery returns discovery client with options from Connection instance.
	// Options provide options replacement for requested discovery client
	// such as endpoint, database, secure connection flag and credentials
	// Options replacement feature not implements now
	Discovery(opts ...Option) discovery.Client
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

func (db *db) Discovery(opts ...Option) discovery.Client {
	return &db.discovery
}

func (db *db) Endpoint() string {
	return db.config.Endpoint()
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

func (db *db) Table(opts ...Option) table.Client {
	return &db.table
}

func (db *db) Scheme(opts ...Option) scheme.Client {
	return &db.scheme
}

func (db *db) Coordination(opts ...Option) coordination.Client {
	return &db.coordination
}

func (db *db) RateLimiter(opts ...Option) ratelimiter.Client {
	return &db.ratelimiter
}

// New connects to name and return name runtime holder
func New(ctx context.Context, opts ...Option) (_ Connection, err error) {
	db := &db{}
	if caFile, has := os.LookupEnv("YDB_SSL_ROOT_CERTIFICATES_FILE"); has {
		opts = append([]Option{WithCertificatesFromFile(caFile)}, opts...)
	}
	if logLevel, has := os.LookupEnv("YDB_LOG_SEVERITY_LEVEL"); has {
		if l := logger.FromString(logLevel); l < logger.QUIET {
			logger := logger.New(
				logger.WithNamespace("ydb"),
				logger.WithMinLevel(logger.FromString(logLevel)),
				logger.WithNoColor(os.Getenv("YDB_LOG_NO_COLOR") != ""),
			)
			opts = append(
				[]Option{
					WithTraceDriver(log.Driver(logger, trace.DetailsAll)),
					WithTraceTable(log.Table(logger, trace.DetailsAll)),
				},
				opts...,
			)
		}
	}
	for _, opt := range opts {
		err = opt(ctx, db)
		if err != nil {
			return nil, err
		}
	}
	db.config = config.New(db.options...)
	onDone := trace.DriverOnInit(db.config.Trace(), &ctx)
	defer func() {
		onDone(err)
	}()
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
