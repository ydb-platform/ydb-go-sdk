package ydb

import (
	"context"
	"os"
	"sync"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/lazy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/proxy"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Connection interface {
	db.Connection

	// Table returns table client with options from Connection instance.
	// Options provide options replacement for requested table client
	// such as endpoint, database, secure connection flag and credentials
	// Options replacement feature not implements now
	Table(opts ...CustomOption) table.Client

	// Scheme returns scheme client with options from Connection instance.
	// Options provide options replacement for requested scheme client
	// such as endpoint, database, secure connection flag and credentials
	// Options replacement feature not implements now
	Scheme(opts ...CustomOption) scheme.Client

	// Coordination returns coordination client with options from Connection instance.
	// Options provide options replacement for requested coordination client
	// such as endpoint, database, secure connection flag and credentials
	// Options replacement feature not implements now
	Coordination(opts ...CustomOption) coordination.Client

	// Ratelimiter returns rate limiter client with options from Connection instance.
	// Options provide options replacement for requested rate limiter client
	// such as endpoint, database, secure connection flag and credentials
	// Options replacement feature not implements now
	Ratelimiter(opts ...CustomOption) ratelimiter.Client

	// Discovery returns discovery client with options from Connection instance.
	// Options provide options replacement for requested discovery client
	// such as endpoint, database, secure connection flag and credentials
	// Options replacement feature not implements now
	Discovery(opts ...CustomOption) discovery.Client

	// Close clears resources and close all connections to YDB
	Close(ctx context.Context) error
}

type connection struct {
	config       config.Config
	options      []config.Option
	tableOptions []tableConfig.Option
	conns        conn.Pool
	mtx          sync.Mutex
	db           db.Connection
	table        table.Client
	scheme       scheme.Client
	discovery    discovery.Client
	coordination coordination.Client
	rateLimiter  ratelimiter.Client
}

func (c *connection) Close(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	var issues []error
	if err := c.discovery.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if err := c.rateLimiter.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if err := c.coordination.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if err := c.scheme.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if err := c.table.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if err := c.db.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if len(issues) > 0 {
		return errors.NewWithIssues("close failed", issues...)
	}
	return nil
}

func (c *connection) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) error {
	return c.db.Invoke(ctx, method, args, reply, opts...)
}

func (c *connection) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return c.db.NewStream(ctx, desc, method, opts...)
}

func (c *connection) Endpoint() string {
	return c.config.Endpoint()
}

func (c *connection) Name() string {
	return c.config.Database()
}

func (c *connection) Secure() bool {
	return c.config.Secure()
}

func (c *connection) Table(opts ...CustomOption) table.Client {
	if len(opts) == 0 {
		return c.table
	}
	return proxy.Table(c.table, c.meta(opts...))
}

func (c *connection) Scheme(opts ...CustomOption) scheme.Client {
	if len(opts) == 0 {
		return c.scheme
	}
	return proxy.Scheme(c.scheme, c.meta(opts...))
}

func (c *connection) Coordination(opts ...CustomOption) coordination.Client {
	if len(opts) == 0 {
		return c.coordination
	}
	return proxy.Coordination(c.coordination, c.meta(opts...))
}

func (c *connection) Ratelimiter(opts ...CustomOption) ratelimiter.Client {
	if len(opts) == 0 {
		return c.rateLimiter
	}
	return proxy.Ratelimiter(c.rateLimiter, c.meta(opts...))
}

func (c *connection) Discovery(opts ...CustomOption) discovery.Client {
	if len(opts) == 0 {
		return c.discovery
	}
	return proxy.Discovery(c.discovery, c.meta(opts...))
}

func (c *connection) meta(opts ...CustomOption) meta.Meta {
	if len(opts) == 0 {
		return c.config.Meta()
	}
	options := customOptions{
		database:    c.config.Database(),
		credentials: c.config.Credentials(),
	}
	for _, o := range opts {
		o(&options)
	}
	return meta.New(
		options.database,
		options.credentials,
		c.config.Trace(),
		c.config.RequestsType(),
	)
}

// New connects to name and return name runtime holder
func New(ctx context.Context, opts ...Option) (_ Connection, err error) {
	c := &connection{}
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
		err = opt(ctx, c)
		if err != nil {
			return nil, err
		}
	}
	c.config = config.New(c.options...)
	c.conns = conn.NewPool(ctx, c.config)
	c.db, err = db.New(ctx, c.config, c.conns)
	if err != nil {
		return nil, err
	}
	c.table = lazy.Table(c.db, c.tableOptions)
	c.scheme = lazy.Scheme(c.db)
	c.discovery = lazy.Discovery(c.db, c.config.Trace())
	c.coordination = lazy.Coordination(c.db)
	c.rateLimiter = lazy.Ratelimiter(c.db)
	return c, nil
}
