package conn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	beginTxFunc func(ctx context.Context, c *conn, txOptions driver.TxOptions) (currentTx, error)
	Parent      interface {
		Table() table.Client
		Scripting() scripting.Client
	}
	conn struct {
		ctx context.Context //nolint:containedctx

		parent     Parent
		trace      *trace.DatabaseSQL
		traceRetry *trace.Retry
		session    table.ClosableSession // Immutable and r/o usage.

		beginTxFuncs map[QueryMode]beginTxFunc

		closed           atomic.Bool
		lastUsage        atomic.Int64
		defaultQueryMode QueryMode

		defaultTxControl *table.TransactionControl
		dataOpts         []options.ExecuteDataQueryOption

		scanOpts []options.ExecuteScanQueryOption

		currentTx     currentTx
		retryBudget   budget.Budget
		bindings      bind.Bindings
		idleThreshold time.Duration
		onClose       []func()
		clock         clockwork.Clock
	}
)

func (c *conn) LastUsage() time.Time {
	return time.Unix(c.lastUsage.Load(), 0)
}

func (c *conn) CheckNamedValue(*driver.NamedValue) error {
	// on this stage allows all values
	return nil
}

func (c *conn) IsValid() bool {
	return c.isReady()
}

type currentTx interface {
	tx.Identifier
	driver.Tx
	driver.ExecerContext
	driver.QueryerContext
	driver.ConnPrepareContext
}

type resultNoRows struct{}

func (resultNoRows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (resultNoRows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

var (
	_ driver.Conn               = &conn{}
	_ driver.ConnPrepareContext = &conn{}
	_ driver.ConnBeginTx        = &conn{}
	_ driver.ExecerContext      = &conn{}
	_ driver.QueryerContext     = &conn{}
	_ driver.Pinger             = &conn{}
	_ driver.Validator          = &conn{}
	_ driver.NamedValueChecker  = &conn{}

	_ driver.Result = resultNoRows{}
)

func New(ctx context.Context, parent Parent, opts ...Option) (*conn, error) {
	s, err := parent.Table().CreateSession(ctx) //nolint:staticcheck
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	cc := &conn{
		ctx:              ctx,
		parent:           parent,
		session:          s,
		defaultQueryMode: DataQueryMode,
		defaultTxControl: table.DefaultTxControl(),
		clock:            clockwork.NewRealClock(),
		trace:            &trace.DatabaseSQL{},
		beginTxFuncs: map[QueryMode]beginTxFunc{
			DataQueryMode: beginTx,
		},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(cc)
		}
	}

	return cc, nil
}

func (c *conn) isReady() bool {
	return c.session.Status() == table.SessionReady
}

func (c *conn) PrepareContext(ctx context.Context, query string) (_ driver.Stmt, finalErr error) {
	if c.currentTx != nil {
		return c.currentTx.PrepareContext(ctx, query)
	}
	onDone := trace.DatabaseSQLOnConnPrepare(c.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*conn).PrepareContext"),
		query,
	)
	defer func() {
		onDone(finalErr)
	}()

	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	return &stmt{
		conn:      c,
		processor: c,
		ctx:       ctx,
		query:     query,
		trace:     c.trace,
	}, nil
}

func (c *conn) execContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (_ driver.Result, finalErr error) {
	defer func() {
		c.lastUsage.Store(c.clock.Now().Unix())
	}()

	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	if c.currentTx != nil {
		return c.currentTx.ExecContext(ctx, query, args)
	}

	m := queryModeFromContext(ctx, c.defaultQueryMode)
	onDone := trace.DatabaseSQLOnConnExec(
		c.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*conn).execContext"),
		query, m.String(), xcontext.IsIdempotent(ctx), c.clock.Since(c.LastUsage()),
	)
	defer func() {
		onDone(finalErr)
	}()

	switch m {
	case DataQueryMode:
		return c.executeDataQuery(ctx, query, args)
	case SchemeQueryMode:
		return c.executeSchemeQuery(ctx, query)
	case ScriptingQueryMode:
		return c.executeScriptingQuery(ctx, query, args)
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute query", m)
	}
}

func (c *conn) executeDataQuery(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	normalizedQuery, parameters, err := c.normalize(query, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	_, res, err := c.session.Execute(ctx,
		txControl(ctx, c.defaultTxControl),
		normalizedQuery, &parameters, c.dataQueryOptions(ctx)...,
	)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	defer res.Close()

	if err := res.NextResultSetErr(ctx); err != nil && !xerrors.Is(err, nil, io.EOF) {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	if err := res.Err(); err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return resultNoRows{}, nil
}

func (c *conn) executeSchemeQuery(ctx context.Context, query string) (driver.Result, error) {
	normalizedQuery, _, err := c.normalize(query)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if err := c.session.ExecuteSchemeQuery(ctx, normalizedQuery); err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return resultNoRows{}, nil
}

func (c *conn) executeScriptingQuery(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	normalizedQuery, parameters, err := c.normalize(query, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	res, err := c.parent.Scripting().StreamExecute(ctx, normalizedQuery, &parameters)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	defer res.Close()

	if err := res.NextResultSetErr(ctx); err != nil && !xerrors.Is(err, nil, io.EOF) {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	if err := res.Err(); err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return resultNoRows{}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, _ error) {
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return c.currentTx.ExecContext(ctx, query, args)
	}

	return c.execContext(ctx, query, args)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, _ error) {
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return c.currentTx.QueryContext(ctx, query, args)
	}

	return c.queryContext(ctx, query, args)
}

func (c *conn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (
	_ driver.Rows, finalErr error,
) {
	defer func() {
		c.lastUsage.Store(c.clock.Now().Unix())
	}()

	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	if c.currentTx != nil {
		return c.currentTx.QueryContext(ctx, query, args)
	}

	var (
		queryMode = queryModeFromContext(ctx, c.defaultQueryMode)
		onDone    = trace.DatabaseSQLOnConnQuery(
			c.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*conn).queryContext"),
			query, queryMode.String(), xcontext.IsIdempotent(ctx), c.clock.Since(c.LastUsage()),
		)
	)
	defer func() {
		onDone(finalErr)
	}()

	normalizedQuery, parameters, err := c.normalize(query, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	switch queryMode {
	case DataQueryMode:
		return c.execDataQuery(ctx, normalizedQuery, parameters)
	case ScanQueryMode:
		return c.execScanQuery(ctx, normalizedQuery, parameters)
	case ExplainQueryMode:
		return c.explainQuery(ctx, normalizedQuery)
	case ScriptingQueryMode:
		return c.execScriptingQuery(ctx, normalizedQuery, parameters)
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' on conn query", queryMode)
	}
}

func (c *conn) execDataQuery(ctx context.Context, query string, params params.Parameters) (driver.Rows, error) {
	_, res, err := c.session.Execute(ctx,
		txControl(ctx, c.defaultTxControl),
		query, &params, c.dataQueryOptions(ctx)...,
	)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	if err = res.Err(); err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return &rows{
		conn:   c,
		result: res,
	}, nil
}

func (c *conn) execScanQuery(ctx context.Context, query string, params params.Parameters) (driver.Rows, error) {
	res, err := c.session.StreamExecuteScanQuery(ctx,
		query, &params, c.scanQueryOptions(ctx)...,
	)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	if err = res.Err(); err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return &rows{
		conn:   c,
		result: res,
	}, nil
}

func (c *conn) explainQuery(ctx context.Context, query string) (driver.Rows, error) {
	exp, err := c.session.Explain(ctx, query)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return &single{
		values: []sql.NamedArg{
			sql.Named("AST", exp.AST),
			sql.Named("Plan", exp.Plan),
		},
	}, nil
}

func (c *conn) execScriptingQuery(ctx context.Context, query string, params params.Parameters) (driver.Rows, error) {
	res, err := c.parent.Scripting().StreamExecute(ctx, query, &params)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	if err = res.Err(); err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return &rows{
		conn:   c,
		result: res,
	}, nil
}

func (c *conn) Ping(ctx context.Context) (finalErr error) {
	onDone := trace.DatabaseSQLOnConnPing(c.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*conn).Ping"),
	)
	defer func() {
		onDone(finalErr)
	}()
	if !c.isReady() {
		return badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if err := c.session.KeepAlive(ctx); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}

	return nil
}

func (c *conn) Close() (finalErr error) {
	if !c.closed.CompareAndSwap(false, true) {
		return badconn.Map(xerrors.WithStackTrace(errConnClosedEarly))
	}

	defer func() {
		for _, onClose := range c.onClose {
			onClose()
		}
	}()

	var (
		ctx    = c.ctx
		onDone = trace.DatabaseSQLOnConnClose(
			c.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*conn).Close"),
		)
	)
	defer func() {
		onDone(finalErr)
	}()
	if c.currentTx != nil {
		_ = c.currentTx.Rollback()
	}
	err := c.session.Close(xcontext.ValueOnly(ctx))
	if err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}

	return nil
}

func (c *conn) Prepare(string) (driver.Stmt, error) {
	return nil, errDeprecated
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errDeprecated
}

func (c *conn) normalize(q string, args ...driver.NamedValue) (query string, _ params.Parameters, _ error) {
	return c.bindings.RewriteQuery(q, func() (ii []interface{}) {
		for i := range args {
			ii = append(ii, args[i])
		}

		return ii
	}()...)
}

func (c *conn) ID() string {
	return c.session.ID()
}

func (c *conn) BeginTx(ctx context.Context, txOptions driver.TxOptions) (_ driver.Tx, finalErr error) {
	var tx currentTx
	onDone := trace.DatabaseSQLOnConnBegin(c.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*conn).BeginTx"),
	)
	defer func() {
		onDone(tx, finalErr)
	}()

	if c.currentTx != nil {
		return nil, badconn.Map(
			xerrors.WithStackTrace(
				fmt.Errorf("broken conn state: conn=%q already have current tx=%q",
					c.ID(), c.currentTx.ID(),
				),
			),
		)
	}

	m := queryModeFromContext(ctx, c.defaultQueryMode)

	beginTx, isKnown := c.beginTxFuncs[m]
	if !isKnown {
		return nil, badconn.Map(
			xerrors.WithStackTrace(
				xerrors.Retryable(
					fmt.Errorf("wrong query mode: %s", m.String()),
					xerrors.InvalidObject(),
					xerrors.WithName("WRONG_QUERY_MODE"),
				),
			),
		)
	}

	var err error
	tx, err = beginTx(ctx, c, txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	c.currentTx = tx

	return tx, nil
}
