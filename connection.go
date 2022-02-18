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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/lazy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/proxy"
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
	db.ConnectionInfo

	// Table returns table client with options from Connection instance.
	// Options provide options replacement for requested table client
	// such as database and access token
	Table(opts ...CustomOption) table.Client

	// Scheme returns scheme client with options from Connection instance.
	// Options provide options replacement for requested scheme client
	// such as database and access token
	Scheme(opts ...CustomOption) scheme.Client

	// Coordination returns coordination client with options from Connection instance.
	// Options provide options replacement for requested coordination client
	// such as database and access token
	Coordination(opts ...CustomOption) coordination.Client

	// Ratelimiter returns rate limiter client with options from Connection instance.
	// Options provide options replacement for requested rate limiter client
	// such as database and access token
	Ratelimiter(opts ...CustomOption) ratelimiter.Client

	// Discovery returns discovery client with options from Connection instance.
	// Options provide options replacement for requested discovery client
	// such as database and access token
	Discovery(opts ...CustomOption) discovery.Client

	// Scripting returns scripting client with options from Connection instance.
	// Options provide options replacement for requested scripting client
	// such as database and access token
	Scripting(opts ...CustomOption) scripting.Client

	// With returns Connection specified with custom options
	// Options provide options replacement for all clients taked from new Connection
	With(opts ...CustomOption) Connection
}

type connection struct {
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

	mtx sync.Mutex
	db  db.Connection
}

func (c *connection) With(opts ...CustomOption) Connection {
	return newProxy(c, newMeta(c.config.Meta(), opts...))
}

func (c *connection) Close(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	var issues []error
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
	return proxy.Table(c.table, newMeta(c.config.Meta(), opts...))
}

func (c *connection) Scheme(opts ...CustomOption) scheme.Client {
	if len(opts) == 0 {
		return c.scheme
	}
	return proxy.Scheme(c.scheme, newMeta(c.config.Meta(), opts...))
}

func (c *connection) Coordination(opts ...CustomOption) coordination.Client {
	if len(opts) == 0 {
		return c.coordination
	}
	return proxy.Coordination(c.coordination, newMeta(c.config.Meta(), opts...))
}

func (c *connection) Ratelimiter(opts ...CustomOption) ratelimiter.Client {
	if len(opts) == 0 {
		return c.ratelimiter
	}
	return proxy.Ratelimiter(c.ratelimiter, newMeta(c.config.Meta(), opts...))
}

func (c *connection) Discovery(opts ...CustomOption) discovery.Client {
	if len(opts) == 0 {
		return c.db.Discovery()
	}
	return proxy.Discovery(c.db.Discovery(), newMeta(c.config.Meta(), opts...))
}

func (c *connection) Scripting(opts ...CustomOption) scripting.Client {
	if len(opts) == 0 {
		return c.scripting
	}
	return proxy.Scripting(c.scripting, newMeta(c.config.Meta(), opts...))
}

// New connects to name and return name runtime holder
func New(ctx context.Context, opts ...Option) (_ Connection, err error) {
	c := &connection{}
	if caFile, has := os.LookupEnv("YDB_SSL_ROOT_CERTIFICATES_FILE"); has {
		opts = append([]Option{WithCertificatesFromFile(caFile)}, opts...)
	}
	if logLevel, has := os.LookupEnv("YDB_LOG_SEVERITY_LEVEL"); has {
		if l := logger.FromString(logLevel); l < logger.QUIET {
			opts = append(
				opts,
				WithLogger(
					trace.DetailsAll,
					WithNamespace("ydb"),
					WithMinLevel(Level(logger.FromString(logLevel))),
					WithNoColor(os.Getenv("YDB_LOG_NO_COLOR") != ""),
				),
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

	c.db, err = db.New(
		ctx,
		c.config,
		append(
			// prepend endpoint, database name and secure options before custom discoveryOptions
			// If custom discoveryOptions contains endpoint, database name or secure options
			// - will apply custom values
			[]discoveryConfig.Option{
				discoveryConfig.WithEndpoint(c.Endpoint()),
				discoveryConfig.WithDatabase(c.Name()),
				discoveryConfig.WithSecure(c.Secure()),
			},
			c.discoveryOptions...,
		)...,
	)
	if err != nil {
		return nil, err
	}

	c.table = lazy.Table(c.db, c.tableOptions)

	c.scheme = lazy.Scheme(c.db, c.schemeOptions)

	c.scripting = lazy.Scripting(c.db, c.scriptingOptions)

	c.coordination = lazy.Coordination(c.db, c.coordinationOptions)

	c.ratelimiter = lazy.Ratelimiter(
		c.db,
		append(
			// prepend operation timeout and cancelAfter options before custom ratelimiterOptions
			// If custom ratelimiterOptions contains operation timeout or cancelAfter options
			// - will apply custom values
			[]ratelimiterConfig.Option{
				ratelimiterConfig.WithOperationTimeout(
					c.config.OperationTimeout(),
				),
				ratelimiterConfig.WithOperationCancelAfter(
					c.config.OperationCancelAfter(),
				),
			},
			c.ratelimiterOptions...,
		),
	)

	return c, nil
}
