package ydb

import (
	"context"
	"os"
	"sync"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	coordinationConfig "github.com/ydb-platform/ydb-go-sdk/v3/coordination/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/single"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/database"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/lazy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	ratelimiterConfig "github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	schemeConfig "github.com/ydb-platform/ydb-go-sdk/v3/scheme/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	scriptingConfig "github.com/ydb-platform/ydb-go-sdk/v3/scripting/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Connection interface provide access to YDB service clients
// Interface and list of clients may be changed in the future
//
// This interface is central part for access to various systems
// embedded to ydb through one configured connection method.
type Connection interface {
	closer.Closer
	database.Info
	grpc.ClientConnInterface

	Close(ctx context.Context) error

	// Method for accessing subsystems
	Table() table.Client
	Scheme() scheme.Client
	Coordination() coordination.Client
	Ratelimiter() ratelimiter.Client
	Discovery() discovery.Client
	Scripting() scripting.Client

	// Make copy with additional options
	With(ctx context.Context, opts ...Option) (Connection, error)
}

type connection struct {
	opts []Option

	config  config.Config
	options []config.Option

	table        table.Client
	tableOptions []tableConfig.Option

	scripting        scripting.Client
	scriptingOptions []scriptingConfig.Option

	scheme        scheme.Client
	schemeOptions []schemeConfig.Option

	discoveryOptions []discoveryConfig.Option

	coordination        coordination.Client
	coordinationOptions []coordinationConfig.Option

	ratelimiter        ratelimiter.Client
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
	children := c.children
	c.children = nil
	c.childrenMtx.Unlock()

	childrenClosers := make([]func(context.Context) error, 0, len(children))
	for _, child := range children {
		childrenClosers = append(childrenClosers, child.Close)
	}

	selfClosers := []func(context.Context) error{
		c.ratelimiter.Close,
		c.coordination.Close,
		c.scheme.Close,
		c.table.Close,
		c.scripting.Close,
		c.db.Close,
		c.pool.Release,
	}

	var issues []error
	for _, closer := range append(childrenClosers, selfClosers...) {
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
	return c.table
}

func (c *connection) Scheme() scheme.Client {
	return c.scheme
}

func (c *connection) Coordination() coordination.Client {
	return c.coordination
}

func (c *connection) Ratelimiter() ratelimiter.Client {
	return c.ratelimiter
}

func (c *connection) Discovery() discovery.Client {
	return c.db.Discovery()
}

func (c *connection) Scripting() scripting.Client {
	return c.scripting
}

// New connects to database and return driver runtime holder
func New(ctx context.Context, opts ...Option) (_ Connection, err error) {
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
		panic("empty dial address")
	}
	if c.config.Database() == "" {
		panic("empty database")
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
			// prepend config params from root config
			[]discoveryConfig.Option{
				discoveryConfig.WithEndpoint(c.Endpoint()),
				discoveryConfig.WithDatabase(c.Name()),
				discoveryConfig.WithSecure(c.Secure()),
				discoveryConfig.WithMeta(c.config.Meta()),
				discoveryConfig.WithOperationTimeout(c.config.OperationTimeout()),
				discoveryConfig.WithOperationCancelAfter(c.config.OperationCancelAfter()),
			},
			c.discoveryOptions...,
		)...,
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	c.table = lazy.Table(
		c.db,
		append(
			// prepend config params from root config
			[]tableConfig.Option{
				tableConfig.WithOperationTimeout(c.config.OperationTimeout()),
				tableConfig.WithOperationCancelAfter(c.config.OperationCancelAfter()),
			},
			c.tableOptions...,
		),
	)

	c.scheme = lazy.Scheme(
		c.db,
		append(
			// prepend config params from root config
			[]schemeConfig.Option{
				schemeConfig.WithOperationTimeout(c.config.OperationTimeout()),
				schemeConfig.WithOperationCancelAfter(c.config.OperationCancelAfter()),
			},
			c.schemeOptions...,
		),
	)

	c.scripting = lazy.Scripting(
		c.db,
		append(
			// prepend config params from root config
			[]scriptingConfig.Option{
				scriptingConfig.WithOperationTimeout(c.config.OperationTimeout()),
				scriptingConfig.WithOperationCancelAfter(c.config.OperationCancelAfter()),
			},
			c.scriptingOptions...,
		),
	)

	c.coordination = lazy.Coordination(
		c.db,
		append(
			// prepend config params from root config
			[]coordinationConfig.Option{
				coordinationConfig.WithOperationTimeout(c.config.OperationTimeout()),
				coordinationConfig.WithOperationCancelAfter(c.config.OperationCancelAfter()),
			},
			c.coordinationOptions...,
		),
	)

	c.ratelimiter = lazy.Ratelimiter(
		c.db,
		append(
			// prepend config params from root config
			[]ratelimiterConfig.Option{
				ratelimiterConfig.WithOperationTimeout(c.config.OperationTimeout()),
				ratelimiterConfig.WithOperationCancelAfter(c.config.OperationCancelAfter()),
			},
			c.ratelimiterOptions...,
		),
	)

	return c, nil
}
