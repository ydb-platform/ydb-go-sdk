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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/lazy"
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
type Connection interface {
	closer.Closer
	db.Info
	grpc.ClientConnInterface

	// Table returns table client
	Table() table.Client

	// Scheme returns scheme client
	Scheme() scheme.Client

	// Coordination returns coordination client
	Coordination() coordination.Client

	// Ratelimiter returns rate limiter client
	Ratelimiter() ratelimiter.Client

	// Discovery returns discovery client
	Discovery() discovery.Client

	// Scripting returns scripting client
	Scripting() scripting.Client

	// With returns Connection specified with custom options
	// Options provide options replacement for all clients taked from new Connection
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
	db  db.Connection

	children    map[uint64]Connection
	childrenMtx sync.Mutex
	onClose     []func(c *connection)
}

func (c *connection) Close(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	defer func() {
		for _, f := range c.onClose {
			f(c)
		}
	}()

	var (
		issues   []error
		children = make([]Connection, 0, len(c.children))
	)

	c.childrenMtx.Lock()
	for _, child := range c.children {
		children = append(children, child)
	}
	c.childrenMtx.Unlock()

	for _, child := range children {
		if err := child.Close(ctx); err != nil {
			issues = append(issues, err)
		}
	}

	if err := c.ratelimiter.Close(ctx); err != nil {
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

	if err := c.scripting.Close(ctx); err != nil {
		issues = append(issues, err)
	}

	if err := c.db.Close(ctx); err != nil {
		issues = append(issues, err)
	}

	if err := c.pool.Release(ctx); err != nil {
		issues = append(issues, err)
	}

	if len(issues) > 0 {
		return errors.WithStackTrace(errors.NewWithIssues("close failed", issues...))
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

// New connects to name and return name runtime holder
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
			return nil, errors.WithStackTrace(err)
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

	c.db, err = db.New(
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
		return nil, errors.WithStackTrace(err)
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
