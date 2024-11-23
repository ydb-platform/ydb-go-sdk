package conn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync/atomic"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/session"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type resultNoRows struct{}

func (resultNoRows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (resultNoRows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

var _ driver.Result = resultNoRows{}

type Parent interface {
	Query() *query.Client
	Trace() *trace.DatabaseSQL
	TraceRetry() *trace.Retry
	RetryBudget() budget.Budget
	Bindings() bind.Bindings
	Clock() clockwork.Clock
}

type currentTx interface {
	tx.Identifier
	driver.Tx
	driver.ExecerContext
	driver.QueryerContext
	driver.ConnPrepareContext
	Rollback() error
}

type Conn struct {
	currentTx
	ctx       context.Context //nolint:containedctx
	parent    Parent
	session   *query.Session
	onClose   []func()
	closed    atomic.Bool
	lastUsage atomic.Int64
}

func New(ctx context.Context, parent Parent, s *query.Session, opts ...Option) *Conn {
	cc := &Conn{
		ctx:     ctx,
		parent:  parent,
		session: s,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(cc)
		}
	}

	return cc
}

func (c *Conn) isReady() bool {
	return c.session.Status() == session.StatusIdle.String()
}

func (c *Conn) normalize(q string, args ...driver.NamedValue) (query string, _ params.Parameters, _ error) {
	queryArgs := make([]any, len(args))
	for i := range args {
		queryArgs[i] = args[i]
	}

	return c.parent.Bindings().RewriteQuery(q, queryArgs...)
}

func (c *Conn) beginTx(ctx context.Context, txOptions driver.TxOptions) (tx currentTx, finalErr error) {
	onDone := trace.DatabaseSQLOnConnBegin(c.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn.(*Conn).beginTx"),
	)
	defer func() {
		onDone(tx, finalErr)
	}()

	if c.currentTx != nil {
		return nil, badconn.Map(
			xerrors.WithStackTrace(xerrors.AlreadyHasTx(c.currentTx.ID())),
		)
	}

	tx, err := beginTx(ctx, c, txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	c.currentTx = tx

	return tx, nil
}

func (c *Conn) execContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (_ driver.Result, finalErr error) {
	defer func() {
		c.lastUsage.Store(c.parent.Clock().Now().Unix())
	}()

	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	if c.currentTx != nil {
		return c.currentTx.ExecContext(ctx, query, args)
	}

	onDone := trace.DatabaseSQLOnConnExec(c.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn.(*Conn).execContext"),
		query, xcontext.UnknownQueryMode.String(), xcontext.IsIdempotent(ctx), c.parent.Clock().Since(c.LastUsage()),
	)
	defer func() {
		onDone(finalErr)
	}()

	normalizedQuery, params, err := c.normalize(query, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	err = c.session.Exec(ctx, normalizedQuery, options.WithParameters(&params))
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return resultNoRows{}, nil
}

func (c *Conn) queryContext(ctx context.Context, queryString string, args []driver.NamedValue) (
	_ driver.Rows, finalErr error,
) {
	defer func() {
		c.lastUsage.Store(c.parent.Clock().Now().Unix())
	}()

	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	if c.currentTx != nil {
		return c.currentTx.QueryContext(ctx, queryString, args)
	}

	onDone := trace.DatabaseSQLOnConnQuery(c.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn.(*Conn).queryContext"),
		queryString, xcontext.UnknownQueryMode.String(), xcontext.IsIdempotent(ctx), c.parent.Clock().Since(c.LastUsage()),
	)

	defer func() {
		onDone(finalErr)
	}()

	normalizedQuery, parameters, err := c.normalize(queryString, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	queryMode := xcontext.QueryModeFromContext(ctx, xcontext.UnknownQueryMode)

	if queryMode == xcontext.ExplainQueryMode {
		return c.queryContextExplain(ctx, normalizedQuery, parameters)
	}

	return c.queryContextOther(ctx, normalizedQuery, parameters)
}

func (c *Conn) queryContextOther(
	ctx context.Context,
	queryString string,
	parameters params.Parameters,
) (driver.Rows, error) {
	res, err := c.session.Query(
		ctx, queryString,
		options.WithParameters(&parameters),
	)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return &rows{
		conn:   c,
		result: res,
	}, nil
}

func (c *Conn) queryContextExplain(
	ctx context.Context,
	queryString string,
	parameters params.Parameters,
) (driver.Rows, error) {
	var ast, plan string
	_, err := c.session.Query(
		ctx, queryString,
		options.WithParameters(&parameters),
		options.WithExecMode(options.ExecModeExplain),
		options.WithStatsMode(options.StatsModeNone, func(stats stats.QueryStats) {
			ast = stats.QueryAST()
			plan = stats.QueryPlan()
		}),
	)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return &single{
		values: []sql.NamedArg{
			sql.Named("AST", ast),
			sql.Named("Plan", plan),
		},
	}, nil
}
