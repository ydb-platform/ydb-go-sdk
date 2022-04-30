package ydb

import (
	"context"
	"errors"
	"os"
	"sync"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/single"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	internalCoordination "github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination"
	coordinationConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/database"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	internalRatelimiter "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	ratelimiterConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/config"
	internalScheme "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	schemeConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/config"
	internalScripting "github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting"
	scriptingConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting/config"
	internalTable "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Connection interface provide access to YDB service clients
// Interface and list of clients may be changed in the future
//
// This interface is central part for access to various systems
// embedded to ydb through one configured connection method.
type Connection interface {
	// ClientConnInterface provide execute unary and streaming RPC over internal balancer
	grpc.ClientConnInterface

	// Endpoint returns initial endpoint
	Endpoint() string

	// Name returns database name
	Name() string

	// Secure returns true if database connection is secure
	Secure() bool

	// Close closes connection and clear resources
	Close(ctx context.Context) error

	// Table returns table client
	Table() table.Client

	// Scheme returns scheme client
	Scheme() scheme.Client

	// Coordination returns coordination client
	Coordination() coordination.Client

	// Ratelimiter returns ratelimiter client
	Ratelimiter() ratelimiter.Client

	// Discovery returns discovery client
	Discovery() discovery.Client

	// Scripting returns scripting client
	Scripting() scripting.Client

	// With makes child connection with the same options and another options
	With(ctx context.Context, opts ...Option) (Connection, error)
}

// nolint: maligned
type connection struct {
	opts []Option

	config  config.Config
	options []config.Option

	tableOnce    sync.Once
	table        *internalTable.Client
	tableOptions []tableConfig.Option

	scriptingOnce    sync.Once
	scripting        *internalScripting.Client
	scriptingOptions []scriptingConfig.Option

	schemeOnce    sync.Once
	scheme        *internalScheme.Client
	schemeOptions []schemeConfig.Option

	discoveryOptions []discoveryConfig.Option

	coordinationOnce    sync.Once
	coordination        *internalCoordination.Client
	coordinationOptions []coordinationConfig.Option

	ratelimiterOnce    sync.Once
	ratelimiter        *internalRatelimiter.Client
	ratelimiterOptions []ratelimiterConfig.Option

	pool conn.Pool

	mtx sync.Mutex
	db  database.Connection

	children    map[uint64]Connection
	childrenMtx sync.Mutex
	onClose     []func(c *connection)

	panicCallback func(e interface{})
}

func (c *connection) Close(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	defer func() {
		for _, f := range c.onClose {
			f(c)
		}
	}()

	c.childrenMtx.Lock()
	closers := make([]func(context.Context) error, 0)
	for _, child := range c.children {
		closers = append(closers, child.Close)
	}
	c.children = nil
	c.childrenMtx.Unlock()

	closers = append(
		closers,
		func(ctx context.Context) error {
			c.ratelimiterOnce.Do(func() {})
			if c.ratelimiter == nil {
				return nil
			}
			return c.ratelimiter.Close(ctx)
		},
		func(ctx context.Context) error {
			c.coordinationOnce.Do(func() {})
			if c.coordination == nil {
				return nil
			}
			return c.coordination.Close(ctx)
		},
		func(ctx context.Context) error {
			c.schemeOnce.Do(func() {})
			if c.scheme == nil {
				return nil
			}
			return c.scheme.Close(ctx)
		},
		func(ctx context.Context) error {
			c.scriptingOnce.Do(func() {})
			if c.scripting == nil {
				return nil
			}
			return c.scripting.Close(ctx)
		},
		func(ctx context.Context) error {
			c.tableOnce.Do(func() {})
			if c.table == nil {
				return nil
			}
			return c.table.Close(ctx)
		},
		c.db.Close,
		c.pool.Release,
	)

	var issues []error
	for _, closer := range closers {
		if err := closer(ctx); err != nil {
			issues = append(issues, err)
		}
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("close failed", issues...))
	}

	return nil
}

func (c *connection) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) (err error) {
	return c.db.Invoke(
		conn.WithoutWrapping(ctx),
		method,
		args,
		reply,
		opts...,
	)
}

func (c *connection) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return c.db.NewStream(
		conn.WithoutWrapping(ctx),
		desc,
		method,
		opts...,
	)
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

func (c *connection) Table() table.Client {
	c.tableOnce.Do(func() {
		c.table = internalTable.New(
			c.db,
			tableConfig.New(
				append(
					// prepend common params from root config
					[]tableConfig.Option{
						tableConfig.With(c.config.Common),
					},
					c.tableOptions...,
				)...,
			),
		)
	})
	// may be nil if driver closed early
	return c.table
}

func (c *connection) Scheme() scheme.Client {
	c.schemeOnce.Do(func() {
		c.scheme = internalScheme.New(
			c.db,
			schemeConfig.New(
				append(
					// prepend common params from root config
					[]schemeConfig.Option{
						schemeConfig.With(c.config.Common),
					},
					c.schemeOptions...,
				)...,
			),
		)
	})
	// may be nil if driver closed early
	return c.scheme
}

func (c *connection) Coordination() coordination.Client {
	c.coordinationOnce.Do(func() {
		c.coordination = internalCoordination.New(
			c.db,
			coordinationConfig.New(
				append(
					// prepend common params from root config
					[]coordinationConfig.Option{
						coordinationConfig.With(c.config.Common),
					},
					c.coordinationOptions...,
				)...,
			),
		)
	})
	// may be nil if driver closed early
	return c.coordination
}

func (c *connection) Ratelimiter() ratelimiter.Client {
	c.ratelimiterOnce.Do(func() {
		c.ratelimiter = internalRatelimiter.New(
			c.db,
			ratelimiterConfig.New(
				append(
					// prepend common params from root config
					[]ratelimiterConfig.Option{
						ratelimiterConfig.With(c.config.Common),
					},
					c.ratelimiterOptions...,
				)...,
			),
		)
	})
	// may be nil if driver closed early
	return c.ratelimiter
}

func (c *connection) Discovery() discovery.Client {
	return c.db.Discovery()
}

func (c *connection) Scripting() scripting.Client {
	c.scriptingOnce.Do(func() {
		c.scripting = internalScripting.New(
			c,
			scriptingConfig.New(
				append(
					// prepend common params from root config
					[]scriptingConfig.Option{
						scriptingConfig.With(c.config.Common),
					},
					c.scriptingOptions...,
				)...,
			),
		)
	})
	// may be nil if driver closed early
	return c.scripting
}

// Open connects to database by DSN and return driver runtime holder
//
// DSN accept connection string like
//
//   "grpc[s]://{endpoint}/?database={database}[&param=value]"
//
// See sugar.DSN helper for make dsn from endpoint and database
func Open(ctx context.Context, dsn string, opts ...Option) (_ Connection, err error) {
	return open(
		ctx,
		append(
			[]Option{
				WithConnectionString(dsn),
			},
			opts...,
		)...,
	)
}

// New connects to database and return driver runtime holder
//
// Deprecated: use Open with required param connectionString instead
func New(ctx context.Context, opts ...Option) (_ Connection, err error) {
	return open(ctx, opts...)
}

func open(ctx context.Context, opts ...Option) (_ Connection, err error) {
	c := &connection{
		opts:     opts,
		children: make(map[uint64]Connection),
	}
	if caFile, has := os.LookupEnv("YDB_SSL_ROOT_CERTIFICATES_FILE"); has {
		opts = append([]Option{WithCertificatesFromFile(caFile)}, opts...)
	}
	if logLevel, has := os.LookupEnv("YDB_LOG_SEVERITY_LEVEL"); has {
		if l := log.FromString(logLevel); l < log.QUIET {
			opts = append(
				opts,
				WithLogger(
					trace.DetailsAll,
					WithNamespace("ydb"),
					WithMinLevel(log.FromString(logLevel)),
					WithNoColor(os.Getenv("YDB_LOG_NO_COLOR") != ""),
				),
			)
		}
	}
	for _, opt := range opts {
		err = opt(ctx, c)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
	}
	c.config = config.New(c.options...)

	if c.config.Endpoint() == "" {
		return nil, xerrors.WithStackTrace(errors.New("configuration: empty dial address"))
	}
	if c.config.Database() == "" {
		return nil, xerrors.WithStackTrace(errors.New("configuration: empty database"))
	}

	if single.IsSingle(c.config.Balancer()) {
		c.discoveryOptions = append(
			c.discoveryOptions,
			discoveryConfig.WithInterval(0),
		)
	}

	if c.pool == nil {
		c.pool = conn.NewPool(
			ctx,
			c.config,
		)
	}

	c.db, err = database.New(
		ctx,
		c.config,
		c.pool,
		append(
			// prepend common params from root config
			[]discoveryConfig.Option{
				discoveryConfig.With(c.config.Common),
				discoveryConfig.WithEndpoint(c.Endpoint()),
				discoveryConfig.WithDatabase(c.Name()),
				discoveryConfig.WithSecure(c.Secure()),
				discoveryConfig.WithMeta(c.config.Meta()),
			},
			c.discoveryOptions...,
		)...,
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return c, nil
}
