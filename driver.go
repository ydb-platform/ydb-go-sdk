package ydb

import (
	"context"
	"errors"
	"fmt"
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
	internalQuery "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	queryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	internalRatelimiter "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	ratelimiterConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/config"
	internalScheme "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	schemeConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/config"
	internalScripting "github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting"
	scriptingConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	internalTable "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	tableConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicclientinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ Connection = (*Driver)(nil)

// Driver type provide access to YDB service clients
type Driver struct {
	ctxCancel context.CancelFunc

	userInfo *dsn.UserInfo

	logger        log.Logger
	loggerOpts    []log.Option
	loggerDetails trace.Detailer

	opts []Option

	config  *config.Config
	options []config.Option

	discovery        *xsync.Once[*internalDiscovery.Client]
	discoveryOptions []discoveryConfig.Option

	table        *xsync.Once[*internalTable.Client]
	tableOptions []tableConfig.Option

	query        *xsync.Once[*internalQuery.Client]
	queryOptions []queryConfig.Option

	scripting        *xsync.Once[*internalScripting.Client]
	scriptingOptions []scriptingConfig.Option

	scheme        *xsync.Once[*internalScheme.Client]
	schemeOptions []schemeConfig.Option

	coordination        *xsync.Once[*internalCoordination.Client]
	coordinationOptions []coordinationConfig.Option

	ratelimiter        *xsync.Once[*internalRatelimiter.Client]
	ratelimiterOptions []ratelimiterConfig.Option

	topic        *xsync.Once[*topicclientinternal.Client]
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

func (d *Driver) trace() *trace.Driver {
	if d.config != nil {
		return d.config.Trace()
	}

	return new(trace.Driver)
}

// Close closes Driver and clear resources
//
//nolint:nonamedreturns
func (d *Driver) Close(ctx context.Context) (finalErr error) {
	onDone := trace.DriverOnClose(d.trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/ydb.(*Driver).Close"),
	)
	defer func() {
		onDone(finalErr)
	}()
	d.ctxCancel()

	d.mtx.Lock()
	defer d.mtx.Unlock()

	d.ctxCancel()

	defer func() {
		for _, f := range d.onClose {
			f(d)
		}
	}()

	closes := make([]func(context.Context) error, 0)
	d.childrenMtx.WithLock(func() {
		for _, child := range d.children {
			closes = append(closes, child.Close)
		}
		d.children = nil
	})

	closes = append(
		closes,
		d.ratelimiter.Close,
		d.coordination.Close,
		d.scheme.Close,
		d.scripting.Close,
		d.table.Close,
		d.query.Close,
		d.topic.Close,
		d.balancer.Close,
		d.pool.Release,
	)

	var issues []error
	for _, f := range closes {
		if err := f(ctx); err != nil {
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
	return d.table.Get()
}

// Query returns query client
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func (d *Driver) Query() query.Client {
	return d.query.Get()
}

// Scheme returns scheme client
func (d *Driver) Scheme() scheme.Client {
	return d.scheme.Get()
}

// Coordination returns coordination client
func (d *Driver) Coordination() coordination.Client {
	return d.coordination.Get()
}

// Ratelimiter returns ratelimiter client
func (d *Driver) Ratelimiter() ratelimiter.Client {
	return d.ratelimiter.Get()
}

// Discovery returns discovery client
func (d *Driver) Discovery() discovery.Client {
	return d.discovery.Get()
}

// Scripting returns scripting client
func (d *Driver) Scripting() scripting.Client {
	return d.scripting.Get()
}

// Topic returns topic client
func (d *Driver) Topic() topic.Client {
	return d.topic.Get()
}

// Open connects to database by DSN and return driver runtime holder
//
// DSN accept Driver string like
//
//	"grpc[s]://{endpoint}/{database}[?param=value]"
//
// See sugar.DSN helper for make dsn from endpoint and database
//
//nolint:nonamedreturns
func Open(ctx context.Context, dsn string, opts ...Option) (_ *Driver, _ error) {
	opts = append(append(make([]Option, 0, len(opts)+1), WithConnectionString(dsn)), opts...)

	for parserIdx := range dsnParsers {
		if parser := dsnParsers[parserIdx]; parser != nil {
			optsFromParser, err := parser(dsn)
			if err != nil {
				return nil, xerrors.WithStackTrace(fmt.Errorf("data source name '%s' wrong: %w", dsn, err))
			}
			opts = append(opts, optsFromParser...)
		}
	}

	d, err := newConnectionFromOptions(ctx, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	onDone := trace.DriverOnInit(
		d.trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/ydb.Open"),
		d.config.Endpoint(), d.config.Database(), d.config.Secure(),
	)
	defer func() {
		onDone(err)
	}()

	if err = d.connect(ctx); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return d, nil
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
// Deprecated: use ydb.Open instead.
// New func have no required arguments, such as connection string.
// Thats why we recognize that New have wrong signature.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func New(ctx context.Context, opts ...Option) (_ *Driver, err error) { //nolint:nonamedreturns
	d, err := newConnectionFromOptions(ctx, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	onDone := trace.DriverOnInit(
		d.trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/ydb.New"),
		d.config.Endpoint(), d.config.Database(), d.config.Secure(),
	)
	defer func() {
		onDone(err)
	}()

	if err = d.connect(ctx); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return d, nil
}

//nolint:cyclop, nonamedreturns
func newConnectionFromOptions(ctx context.Context, opts ...Option) (_ *Driver, err error) {
	ctx, driverCtxCancel := xcontext.WithCancel(xcontext.ValueOnly(ctx))
	defer func() {
		if err != nil {
			driverCtxCancel()
		}
	}()

	d := new(Driver)
	d.children = make(map[uint64]*Driver)
	d.ctxCancel = driverCtxCancel

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
			WithTraceDriver(log.Driver(d.logger, d.loggerDetails, d.loggerOpts...)),       //nolint:contextcheck
			WithTraceTable(log.Table(d.logger, d.loggerDetails, d.loggerOpts...)),         //nolint:contextcheck
			WithTraceQuery(log.Query(d.logger, d.loggerDetails, d.loggerOpts...)),         //nolint:contextcheck
			WithTraceScripting(log.Scripting(d.logger, d.loggerDetails, d.loggerOpts...)), //nolint:contextcheck
			WithTraceScheme(log.Scheme(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceCoordination(log.Coordination(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceRatelimiter(log.Ratelimiter(d.logger, d.loggerDetails, d.loggerOpts...)),
			WithTraceDiscovery(log.Discovery(d.logger, d.loggerDetails, d.loggerOpts...)),     //nolint:contextcheck
			WithTraceTopic(log.Topic(d.logger, d.loggerDetails, d.loggerOpts...)),             //nolint:contextcheck
			WithTraceDatabaseSQL(log.DatabaseSQL(d.logger, d.loggerDetails, d.loggerOpts...)), //nolint:contextcheck
			WithTraceRetry(log.Retry(d.logger, d.loggerDetails, d.loggerOpts...)),             //nolint:contextcheck
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

//nolint:cyclop, nonamedreturns, funlen
func (d *Driver) connect(ctx context.Context) (err error) {
	if d.config.Endpoint() == "" {
		return xerrors.WithStackTrace(errors.New("configuration: empty dial address")) //nolint:goerr113
	}

	if d.config.Database() == "" {
		return xerrors.WithStackTrace(errors.New("configuration: empty database")) //nolint:goerr113
	}

	if d.userInfo != nil {
		d.config = d.config.With(config.WithCredentials(
			credentials.NewStaticCredentials(
				d.userInfo.User, d.userInfo.Password,
				d.config.Endpoint(),
				credentials.WithGrpcDialOptions(d.config.GrpcDialOptions()...),
			),
		))
	}

	if d.pool == nil {
		d.pool = conn.NewPool(ctx, d.config)
	}

	d.balancer, err = balancer.New(ctx, d.config, d.pool, d.discoveryOptions...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	d.table = xsync.OnceValue(func() *internalTable.Client {
		return internalTable.New(xcontext.ValueOnly(ctx),
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
	})

	d.query = xsync.OnceValue(func() *internalQuery.Client {
		return internalQuery.New(xcontext.ValueOnly(ctx),
			d.balancer,
			queryConfig.New(
				append(
					// prepend common params from root config
					[]queryConfig.Option{
						queryConfig.With(d.config.Common),
					},
					d.queryOptions...,
				)...,
			),
		)
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	d.scheme = xsync.OnceValue(func() *internalScheme.Client {
		return internalScheme.New(xcontext.ValueOnly(ctx),
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
	})

	d.coordination = xsync.OnceValue(func() *internalCoordination.Client {
		return internalCoordination.New(xcontext.ValueOnly(ctx),
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
	})

	d.ratelimiter = xsync.OnceValue(func() *internalRatelimiter.Client {
		return internalRatelimiter.New(xcontext.ValueOnly(ctx),
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
	})

	d.discovery = xsync.OnceValue(func() *internalDiscovery.Client {
		return internalDiscovery.New(xcontext.ValueOnly(ctx),
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
	})

	d.scripting = xsync.OnceValue(func() *internalScripting.Client {
		return internalScripting.New(xcontext.ValueOnly(ctx),
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
	})

	d.topic = xsync.OnceValue(func() *topicclientinternal.Client {
		return topicclientinternal.New(xcontext.ValueOnly(ctx),
			d.balancer,
			d.config.Credentials(),
			append(
				// prepend common params from root config
				[]topicoptions.TopicOption{
					topicoptions.WithOperationTimeout(d.config.OperationTimeout()),
					topicoptions.WithOperationCancelAfter(d.config.OperationCancelAfter()),
				},
				d.topicOptions...,
			)...,
		)
	})

	return nil
}

// GRPCConn casts *ydb.Driver to grpc.ClientConnInterface for executing
// unary and streaming RPC over internal driver balancer.
//
// Warning: for connect to driver-unsupported YDB services
func GRPCConn(cc *Driver) grpc.ClientConnInterface {
	return conn.WithContextModifier(cc.balancer, conn.WithoutWrapping)
}
