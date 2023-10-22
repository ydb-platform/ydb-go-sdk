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

	config  *config.Config
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
func (d *Driver) Close(ctx context.Context) error {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	defer func() {
		for _, f := range d.onClose {
			f(d)
		}
	}()

	closers := make([]func(context.Context) error, 0)
	d.childrenMtx.WithLock(func() {
		for _, child := range d.children {
			closers = append(closers, child.Close)
		}
		d.children = nil
	})

	closers = append(
		closers,
		d.ratelimiterOnce.Close,
		d.coordinationOnce.Close,
		d.schemeOnce.Close,
		d.scriptingOnce.Close,
		d.tableOnce.Close,
		d.topicOnce.Close,
		d.balancer.Close,
		d.pool.Release,
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
func (d *Driver) Endpoint() string {
	return d.config.Endpoint()
}

// Name returns database name
func (d *Driver) Name() string {
	return d.config.Database()
}

// Secure returns true if database Driver is secure
func (d *Driver) Secure() bool {
	return d.config.Secure()
}

// Table returns table client
func (d *Driver) Table() table.Client {
	d.tableOnce.Init(func() closeFunc {
		d.table = internalTable.New(
			d.balancer,
			tableConfig.New(
				append(
					// prepend common params from root config
					[]tableConfig.Option{
						tableConfig.With(d.config.Common),
					},
					d.tableOptions...,
				)...,
			),
		)
		return d.table.Close
	})
	// may be nil if driver closed early
	return d.table
}

// Scheme returns scheme client
func (d *Driver) Scheme() scheme.Client {
	d.schemeOnce.Init(func() closeFunc {
		d.scheme = internalScheme.New(
			d.balancer,
			schemeConfig.New(
				append(
					// prepend common params from root config
					[]schemeConfig.Option{
						schemeConfig.WithDatabaseName(d.Name()),
						schemeConfig.With(d.config.Common),
					},
					d.schemeOptions...,
				)...,
			),
		)
		return d.scheme.Close
	})
	// may be nil if driver closed early
	return d.scheme
}

// Coordination returns coordination client
func (d *Driver) Coordination() coordination.Client {
	d.coordinationOnce.Init(func() closeFunc {
		d.coordination = internalCoordination.New(
			d.balancer,
			coordinationConfig.New(
				append(
					// prepend common params from root config
					[]coordinationConfig.Option{
						coordinationConfig.With(d.config.Common),
					},
					d.coordinationOptions...,
				)...,
			),
		)
		return d.coordination.Close
	})
	// may be nil if driver closed early
	return d.coordination
}

// Ratelimiter returns ratelimiter client
func (d *Driver) Ratelimiter() ratelimiter.Client {
	d.ratelimiterOnce.Init(func() closeFunc {
		d.ratelimiter = internalRatelimiter.New(
			d.balancer,
			ratelimiterConfig.New(
				append(
					// prepend common params from root config
					[]ratelimiterConfig.Option{
						ratelimiterConfig.With(d.config.Common),
					},
					d.ratelimiterOptions...,
				)...,
			),
		)
		return d.ratelimiter.Close
	})
	// may be nil if driver closed early
	return d.ratelimiter
}

// Discovery returns discovery client
func (d *Driver) Discovery() discovery.Client {
	d.discoveryOnce.Init(func() closeFunc {
		d.discovery = internalDiscovery.New(
			d.pool.Get(endpoint.New(d.config.Endpoint())),
			discoveryConfig.New(
				append(
					// prepend common params from root config
					[]discoveryConfig.Option{
						discoveryConfig.With(d.config.Common),
						discoveryConfig.WithEndpoint(d.Endpoint()),
						discoveryConfig.WithDatabase(d.Name()),
						discoveryConfig.WithSecure(d.Secure()),
						discoveryConfig.WithMeta(d.config.Meta()),
					},
					d.discoveryOptions...,
				)...,
			),
		)
		return d.discovery.Close
	})
	// may be nil if driver closed early
	return d.discovery
}

// Scripting returns scripting client
func (d *Driver) Scripting() scripting.Client {
	d.scriptingOnce.Init(func() closeFunc {
		d.scripting = internalScripting.New(
			d.balancer,
			scriptingConfig.New(
				append(
					// prepend common params from root config
					[]scriptingConfig.Option{
						scriptingConfig.With(d.config.Common),
					},
					d.scriptingOptions...,
				)...,
			),
		)
		return d.scripting.Close
	})
	// may be nil if driver closed early
	return d.scripting
}

// Topic returns topic client
func (d *Driver) Topic() topic.Client {
	d.topicOnce.Init(func() closeFunc {
		d.topic = topicclientinternal.New(d.balancer, d.config.Credentials(),
			append(
				// prepend common params from root config
				[]topicoptions.TopicOption{
					topicoptions.WithOperationTimeout(d.config.OperationTimeout()),
					topicoptions.WithOperationCancelAfter(d.config.OperationCancelAfter()),
				},
				d.topicOptions...,
			)...,
		)
		return d.topic.Close
	})
	return d.topic
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
				credentials.WithGrpcDialOptions(c.config.GrpcDialOptions()...),
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
