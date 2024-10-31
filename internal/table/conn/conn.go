package conn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"slices"
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
	Parent interface {
		Table() table.Client
		Scripting() scripting.Client

		Trace() *trace.DatabaseSQL
		TraceRetry() *trace.Retry
		RetryBudget() budget.Budget
		Bindings() bind.Bindings
		Clock() clockwork.Clock
	}
	Conn struct {
		ctx context.Context //nolint:containedctx

		parent  Parent
		session table.ClosableSession // Immutable and r/o usage.

		fakeTxModes []QueryMode

		closed           atomic.Bool
		lastUsage        atomic.Int64
		defaultQueryMode QueryMode

		defaultTxControl *table.TransactionControl
		dataOpts         []options.ExecuteDataQueryOption

		scanOpts []options.ExecuteScanQueryOption

		currentTx     currentTx
		idleThreshold time.Duration
		onClose       []func()
	}
)

func (c *Conn) LastUsage() time.Time {
	return time.Unix(c.lastUsage.Load(), 0)
}

func (c *Conn) CheckNamedValue(*driver.NamedValue) error {
	// on this stage allows all values
	return nil
}

func (c *Conn) IsValid() bool {
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
	_ driver.Conn               = &Conn{}
	_ driver.ConnPrepareContext = &Conn{}
	_ driver.ConnBeginTx        = &Conn{}
	_ driver.ExecerContext      = &Conn{}
	_ driver.QueryerContext     = &Conn{}
	_ driver.Pinger             = &Conn{}
	_ driver.Validator          = &Conn{}
	_ driver.NamedValueChecker  = &Conn{}

	_ driver.Result = resultNoRows{}
)

func New(ctx context.Context, parent Parent, s table.ClosableSession, opts ...Option) *Conn {
	cc := &Conn{
		ctx:              ctx,
		parent:           parent,
		session:          s,
		defaultQueryMode: DataQueryMode,
		defaultTxControl: table.DefaultTxControl(),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(cc)
		}
	}

	return cc
}

func (c *Conn) isReady() bool {
	return c.session.Status() == table.SessionReady
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (_ driver.Stmt, finalErr error) {
	if c.currentTx != nil {
		return c.currentTx.PrepareContext(ctx, query)
	}
	onDone := trace.DatabaseSQLOnConnPrepare(c.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*Conn).PrepareContext"),
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
	}, nil
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

	m := queryModeFromContext(ctx, c.defaultQueryMode)
	onDone := trace.DatabaseSQLOnConnExec(c.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*Conn).execContext"),
		query, m.String(), xcontext.IsIdempotent(ctx), c.parent.Clock().Since(c.LastUsage()),
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

func (c *Conn) executeDataQuery(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
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

func (c *Conn) executeSchemeQuery(ctx context.Context, query string) (driver.Result, error) {
	normalizedQuery, _, err := c.normalize(query)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if err := c.session.ExecuteSchemeQuery(ctx, normalizedQuery); err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}

	return resultNoRows{}, nil
}

func (c *Conn) executeScriptingQuery(
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

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, _ error) {
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return c.currentTx.ExecContext(ctx, query, args)
	}

	return c.execContext(ctx, query, args)
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, _ error) {
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return c.currentTx.QueryContext(ctx, query, args)
	}

	return c.queryContext(ctx, query, args)
}

func (c *Conn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (
	_ driver.Rows, finalErr error,
) {
	defer func() {
		c.lastUsage.Store(c.parent.Clock().Now().Unix())
	}()

	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	if c.currentTx != nil {
		return c.currentTx.QueryContext(ctx, query, args)
	}

	var (
		queryMode = queryModeFromContext(ctx, c.defaultQueryMode)
		onDone    = trace.DatabaseSQLOnConnQuery(c.parent.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*Conn).queryContext"),
			query, queryMode.String(), xcontext.IsIdempotent(ctx), c.parent.Clock().Since(c.LastUsage()),
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
		return nil, fmt.Errorf("unsupported query mode '%s' on Conn query", queryMode)
	}
}

func (c *Conn) execDataQuery(ctx context.Context, query string, params params.Parameters) (driver.Rows, error) {
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

func (c *Conn) execScanQuery(ctx context.Context, query string, params params.Parameters) (driver.Rows, error) {
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

func (c *Conn) explainQuery(ctx context.Context, query string) (driver.Rows, error) {
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

func (c *Conn) execScriptingQuery(ctx context.Context, query string, params params.Parameters) (driver.Rows, error) {
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

func (c *Conn) Ping(ctx context.Context) (finalErr error) {
	onDone := trace.DatabaseSQLOnConnPing(c.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*Conn).Ping"),
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

func (c *Conn) Close() (finalErr error) {
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
		onDone = trace.DatabaseSQLOnConnClose(c.parent.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*Conn).Close"),
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

func (c *Conn) Prepare(string) (driver.Stmt, error) {
	return nil, errDeprecated
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, errDeprecated
}

func (c *Conn) normalize(q string, args ...driver.NamedValue) (query string, _ params.Parameters, _ error) {
	return c.parent.Bindings().RewriteQuery(q, func() (ii []interface{}) {
		for i := range args {
			ii = append(ii, args[i])
		}

		return ii
	}()...)
}

func (c *Conn) ID() string {
	return c.session.ID()
}

func (c *Conn) beginTx(ctx context.Context, txOptions driver.TxOptions) (currentTx, error) {
	if c.currentTx != nil {
		return nil, badconn.Map(
			xerrors.WithStackTrace(
				fmt.Errorf("broken Conn state: Conn=%q already have current tx=%q",
					c.ID(), c.currentTx.ID(),
				),
			),
		)
	}

	m := queryModeFromContext(ctx, c.defaultQueryMode)

	if slices.Contains(c.fakeTxModes, m) {
		return beginTxFake(ctx, c), nil
	}

	tx, err := beginTx(ctx, c, txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return tx, nil
}

func (c *Conn) BeginTx(ctx context.Context, txOptions driver.TxOptions) (driver.Tx, error) {
	onDone := trace.DatabaseSQLOnConnBegin(c.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*Conn).BeginTx"),
	)
	tx, err := c.beginTx(ctx, txOptions)
	defer func() {
		onDone(tx, err)
	}()

	c.currentTx = tx

	return tx, nil
}
