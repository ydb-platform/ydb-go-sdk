package xsql

import (
	"context"
	"database/sql/driver"
	"io"
	"os"
	"strconv"

	"github.com/jonboulle/clockwork"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	internalQuery "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/xquery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/xtable"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ io.Closer        = (*Connector)(nil)
	_ driver.Connector = (*Connector)(nil)
)

type (
	Engine    uint8
	Connector struct {
		parent      ydbDriver
		balancer    grpc.ClientConnInterface
		queryConfig *config.Config

		processor Engine

		TableOpts             []xtable.Option
		QueryOpts             []xquery.Option
		disableServerBalancer bool
		onClose               []func(*Connector)

		clock          clockwork.Clock
		done           chan struct{}
		trace          *trace.DatabaseSQL
		traceRetry     *trace.Retry
		retryBudget    budget.Budget
		pathNormalizer bind.TablePathPrefix
		bindings       bind.Bindings
	}
	ydbDriver interface {
		Name() string
		Table() table.Client
		Query() query.Client
		Scripting() scripting.Client
		Scheme() scheme.Client
	}
)

func (e Engine) String() string {
	switch e {
	case TABLE:
		return "TABLE"
	case QUERY:
		return "QUERY"
	default:
		return "UNKNOWN"
	}
}

func (c *Connector) Parent() ydbDriver {
	return c.parent
}

func (c *Connector) RetryBudget() budget.Budget {
	return c.retryBudget
}

func (c *Connector) Bindings() bind.Bindings {
	return c.bindings
}

func (c *Connector) Clock() clockwork.Clock {
	return c.clock
}

func (c *Connector) Trace() *trace.DatabaseSQL {
	return c.trace
}

func (c *Connector) TraceRetry() *trace.Retry {
	return c.traceRetry
}

const (
	QUERY = iota + 1
	TABLE
)

func (c *Connector) Open(name string) (driver.Conn, error) {
	return nil, xerrors.WithStackTrace(driver.ErrSkip)
}

func (c *Connector) Connect(ctx context.Context) (_ driver.Conn, finalErr error) { //nolint:funlen
	onDone := trace.DatabaseSQLOnConnectorConnect(c.Trace(), &ctx,
		stack.FunctionID("database/sql.(*Connector).Connect", stack.Package("database/sql")),
	)

	if !c.disableServerBalancer {
		ctx = meta.WithAllowFeatures(ctx, meta.HintSessionBalancer)
	}

	switch c.processor {
	case QUERY:
		s, err := internalQuery.CreateSession(ctx, Ydb_Query_V1.NewQueryServiceClient(c.balancer), c.queryConfig)
		defer func() {
			onDone(s, finalErr)
		}()
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		conn := &Conn{
			processor: QUERY,
			cc:        xquery.New(ctx, s, c.QueryOpts...),
			ctx:       ctx,
			connector: c,
			lastUsage: xsync.NewLastUsage(xsync.WithClock(c.Clock())),
		}

		return conn, nil

	case TABLE:
		s, err := c.parent.Table().CreateSession(ctx) //nolint:staticcheck
		defer func() {
			onDone(s, finalErr)
		}()
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		conn := &Conn{
			processor: TABLE,
			cc:        xtable.New(ctx, c.parent.Scripting(), s, c.TableOpts...),
			ctx:       ctx,
			connector: c,
			lastUsage: xsync.NewLastUsage(xsync.WithClock(c.Clock())),
		}

		return conn, nil
	default:
		return nil, xerrors.WithStackTrace(errWrongQueryProcessor)
	}
}

func (c *Connector) Driver() driver.Driver {
	return c
}

func (c *Connector) Close() error {
	select {
	case <-c.done:
		return nil
	default:
		close(c.done)

		for _, onClose := range c.onClose {
			onClose(c)
		}

		return nil
	}
}

func Open(
	parent ydbDriver,
	balancer grpc.ClientConnInterface,
	queryConfig *config.Config,
	opts ...Option,
) (_ *Connector, err error) {
	c := &Connector{
		parent:      parent,
		balancer:    balancer,
		queryConfig: queryConfig,
		processor: func() Engine {
			overQueryService, err := strconv.ParseBool(os.Getenv("YDB_DATABASE_SQL_OVER_QUERY_SERVICE"))
			if err == nil && !overQueryService {
				return TABLE
			}

			// default is Query Engine
			return QUERY
		}(),
		clock:          clockwork.NewRealClock(),
		done:           make(chan struct{}),
		trace:          &trace.DatabaseSQL{},
		traceRetry:     &trace.Retry{},
		pathNormalizer: bind.TablePathPrefix(parent.Name()),
	}

	for _, opt := range opts {
		if opt != nil {
			if err = opt.Apply(c); err != nil {
				return nil, err
			}
		}
	}

	return c, nil
}
