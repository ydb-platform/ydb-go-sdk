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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	internalCoordination "github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination"
	coordinationConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	internalDiscovery "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dsn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	internalRatelimiter "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	ratelimiterConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/config"
	internalScheme "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	schemeConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/config"
	internalScripting "github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting"
	scriptingConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting/config"
	internalTable "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicclientinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Connection interface provide access to YDB service clients
// Interface and list of clients may be changed in the future
//
// Deprecated: use directly *Driver type from ydb.Open instead
type Connection interface {
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

	// Topic returns topic client
	Topic() topic.Client
}

var _ Connection = (*Driver)(nil)

// Driver type provide access to YDB service clients
type Driver struct { //nolint:maligned
	userInfo *dsn.UserInfo

	logger        log.Logger
	loggerOpts    []log.Option
	loggerDetails trace.Detailer

	opts []Option

	config  config.Config
	options []config.Option

	discoveryOnce    initOnce
	discovery        *internalDiscovery.Client
	discoveryOptions []discoveryConfig.Option

	tableOnce    initOnce
	table        *internalTable.Client
	tableOptions []tableConfig.Option

	scriptingOnce    initOnce
	scripting        *internalScripting.Client
	scriptingOptions []scriptingConfig.Option

	schemeOnce    initOnce
	scheme        *internalScheme.Client
	schemeOptions []schemeConfig.Option

	coordinationOnce    initOnce
	coordination        *internalCoordination.Client
	coordinationOptions []coordinationConfig.Option

	ratelimiterOnce    initOnce
	ratelimiter        *internalRatelimiter.Client
	ratelimiterOptions []ratelimiterConfig.Option

	topicOnce    initOnce
	topic        *topicclientinternal.Client
	topicOptions []topicoptions.TopicOption

	databaseSQLOptions []xsql.ConnectorOption

	pool *conn.Pool

	mtx      sync.Mutex
	balancer *balancer.Balancer

	children    map[uint64]*Driver
	childrenMtx xsync.Mutex
	onClose     []func(c *Driver)

	panicCallback func(e interface{})
}

// Close closes Driver and clear resources
func (c *Driver) Close(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	defer func() {
		for _, f := range c.onClose {
			f(c)
		}
	}()

	closers := make([]func(context.Context) error, 0)
	c.childrenMtx.WithLock(func() {
		for _, child := range c.children {
			closers = append(closers, child.Close)
		}
		c.children = nil
	})

	closers = append(
		closers,
		c.ratelimiterOnce.Close,
		c.coordinationOnce.Close,
		c.schemeOnce.Close,
		c.scriptingOnce.Close,
		c.tableOnce.Close,
		c.topicOnce.Close,
		c.balancer.Close,
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

// Endpoint returns initial endpoint
func (c *Driver) Endpoint() string {
	return c.config.Endpoint()
}

// Name returns database name
func (c *Driver) Name() string {
	return c.config.Database()
}

// Secure returns true if database Driver is secure
func (c *Driver) Secure() bool {
	return c.config.Secure()
}

// Table returns table client
func (c *Driver) Table() table.Client {
	c.tableOnce.Init(func() closeFunc {
		c.table = internalTable.New(
			c.balancer,
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
		return c.table.Close
	})
	// may be nil if driver closed early
	return c.table
}

// Scheme returns scheme client
func (c *Driver) Scheme() scheme.Client {
	c.schemeOnce.Init(func() closeFunc {
		c.scheme = internalScheme.New(
			c.balancer,
			schemeConfig.New(
				append(
					// prepend common params from root config
					[]schemeConfig.Option{
						schemeConfig.WithDatabaseName(c.Name()),
						schemeConfig.With(c.config.Common),
					},
					c.schemeOptions...,
				)...,
			),
		)
		return c.scheme.Close
	})
	// may be nil if driver closed early
	return c.scheme
}

// Coordination returns coordination client
func (c *Driver) Coordination() coordination.Client {
	c.coordinationOnce.Init(func() closeFunc {
		c.coordination = internalCoordination.New(
			c.balancer,
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
		return c.coordination.Close
	})
	// may be nil if driver closed early
	return c.coordination
}

// Ratelimiter returns ratelimiter client
func (c *Driver) Ratelimiter() ratelimiter.Client {
	c.ratelimiterOnce.Init(func() closeFunc {
		c.ratelimiter = internalRatelimiter.New(
			c.balancer,
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
		return c.ratelimiter.Close
	})
	// may be nil if driver closed early
	return c.ratelimiter
}

// Discovery returns discovery client
func (c *Driver) Discovery() discovery.Client {
	c.discoveryOnce.Init(func() closeFunc {
		c.discovery = internalDiscovery.New(
			c.pool.Get(endpoint.New(c.config.Endpoint())),
			discoveryConfig.New(
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
			),
		)
		return c.discovery.Close
	})
	// may be nil if driver closed early
	return c.discovery
}

// Scripting returns scripting client
func (c *Driver) Scripting() scripting.Client {
	c.scriptingOnce.Init(func() closeFunc {
		c.scripting = internalScripting.New(
			c.balancer,
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
		return c.scripting.Close
	})
	// may be nil if driver closed early
	return c.scripting
}

// Topic returns topic client
func (c *Driver) Topic() topic.Client {
	c.topicOnce.Init(func() closeFunc {
		c.topic = topicclientinternal.New(c.balancer, c.config.Credentials(),
			append(
				// prepend common params from root config
				[]topicoptions.TopicOption{
					topicoptions.WithOperationTimeout(c.config.OperationTimeout()),
					topicoptions.WithOperationCancelAfter(c.config.OperationCancelAfter()),
				},
				c.topicOptions...,
			)...,
		)
		return c.topic.Close
	})
	return c.topic
}

// Open connects to database by DSN and return driver runtime holder
//
// DSN accept Driver string like
//
//	"grpc[s]://{endpoint}/{database}[?param=value]"
//
// See sugar.DSN helper for make dsn from endpoint and database
func Open(ctx context.Context, dsn string, opts ...Option) (_ *Driver, err error) {
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

func MustOpen(ctx context.Context, dsn string, opts ...Option) *Driver {
	db, err := Open(ctx, dsn, opts...)
	if err != nil {
		panic(err)
	}
	return db
}

// New connects to database and return driver runtime holder
//
// Deprecated: use Open with required param connectionString instead
func New(ctx context.Context, opts ...Option) (_ *Driver, err error) {
	return open(ctx, opts...)
}

func newConnectionFromOptions(ctx context.Context, opts ...Option) (_ *Driver, err error) {
	d := &Driver{
		children: make(map[uint64]*Driver),
	}
	if caFile, has := os.LookupEnv("YDB_SSL_ROOT_CERTIFICATES_FILE"); has {
		d.opts = append(d.opts,
			WithCertificatesFromFile(caFile),
		)
	}
	if logLevel, has := os.LookupEnv("YDB_LOG_SEVERITY_LEVEL"); has {
		if l := log.FromString(logLevel); l < log.QUIET {
			d.opts = append(d.opts,
				WithLogger(
					log.Default(os.Stderr,
						log.WithMinLevel(log.FromString(logLevel)),
						log.WithColoring(),
					),
					trace.MatchDetails(
						os.Getenv("YDB_LOG_DETAILS"),
						trace.WithDefaultDetails(trace.DetailsAll),
					),
					log.WithLogQuery(),
				),
			)
		}
	}
	d.opts = append(d.opts, opts...)
	for _, opt := range d.opts {
		if opt != nil {
			err = opt(ctx, d)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
		}
	}
	if d.logger != nil {
		for _, opt := range []Option{
			WithTraceDriver(log.Driver(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceTable(log.Table(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceScripting(log.Scripting(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceScheme(log.Scheme(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceCoordination(log.Coordination(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceRatelimiter(log.Ratelimiter(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceDiscovery(log.Discovery(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceTopic(log.Topic(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceDatabaseSQL(log.DatabaseSQL(d.logger, d.loggerDetails, d.loggerOpts...)),
		} {
			if opt != nil {
				err = opt(ctx, d)
				if err != nil {
					return nil, xerrors.WithStackTrace(err)
				}
			}
		}
	}
	d.config = config.New(d.options...)
	return d, nil
}

func connect(ctx context.Context, c *Driver) error {
	var err error

	if c.config.Endpoint() == "" {
		return xerrors.WithStackTrace(errors.New("configuration: empty dial address"))
	}
	if c.config.Database() == "" {
		return xerrors.WithStackTrace(errors.New("configuration: empty database"))
	}

	onDone := trace.DriverOnInit(
		c.config.Trace(),
		&ctx,
		c.config.Endpoint(),
		c.config.Database(),
		c.config.Secure(),
	)
	defer func() {
		onDone(err)
	}()

	if c.userInfo != nil {
		c.config = c.config.With(config.WithCredentials(
			credentials.NewStaticCredentials(
				c.userInfo.User, c.userInfo.Password,
				c.config.Endpoint(),
				c.config.GrpcDialOptions()...,
			),
		))
	}

	if c.pool == nil {
		c.pool = conn.NewPool(c.config)
	}

	c.balancer, err = balancer.New(ctx, c.config, c.pool, c.discoveryOptions...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func open(ctx context.Context, opts ...Option) (_ *Driver, err error) {
	c, err := newConnectionFromOptions(ctx, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	err = connect(ctx, c)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return c, nil
}

// GRPCConn casts *ydb.Driver to grpc.ClientConnInterface for executing
// unary and streaming RPC over internal driver balancer.
//
// Warning: for connect to driver-unsupported YDB services
func GRPCConn(cc *Driver) grpc.ClientConnInterface {
	return conn.WithContextModifier(cc.balancer, conn.WithoutWrapping)
}

// Helper types for closing lazy clients
type closeFunc func(ctx context.Context) error

type initOnce struct {
	once  sync.Once
	close closeFunc
}

func (lo *initOnce) Init(f func() closeFunc) {
	lo.once.Do(func() {
		lo.close = f()
	})
}

func (lo *initOnce) Close(ctx context.Context) error {
	lo.once.Do(func() {})
	if lo.close == nil {
		return nil
	}
	return lo.close(ctx)
}
