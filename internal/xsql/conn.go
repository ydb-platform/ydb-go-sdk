package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync/atomic"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/isolation"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type connOption func(*conn)

func withDataOpts(dataOpts ...options.ExecuteDataQueryOption) connOption {
	return func(c *conn) {
		c.dataOpts = dataOpts
	}
}

func withScanOpts(scanOpts ...options.ExecuteScanQueryOption) connOption {
	return func(c *conn) {
		c.scanOpts = scanOpts
	}
}

func withDefaultTxControl(defaultTxControl *table.TransactionControl) connOption {
	return func(c *conn) {
		c.defaultTxControl = defaultTxControl
	}
}

func withDefaultQueryMode(mode QueryMode) connOption {
	return func(c *conn) {
		c.defaultQueryMode = mode
	}
}

func withTrace(t trace.SQL) connOption {
	return func(c *conn) {
		c.trace = t
	}
}

type conn struct {
	nopResult
	namedValueChecker

	connector *Connector
	session   table.ClosableSession // Immutable and r/o usage.
	closed    uint32

	defaultQueryMode QueryMode
	defaultTxControl *table.TransactionControl

	dataOpts []options.ExecuteDataQueryOption
	scanOpts []options.ExecuteScanQueryOption

	transaction table.Transaction

	trace trace.SQL
}

var (
	_ driver.Conn               = &conn{}
	_ driver.ConnPrepareContext = &conn{}
	_ driver.ConnBeginTx        = &conn{}
	_ driver.ExecerContext      = &conn{}
	_ driver.QueryerContext     = &conn{}
	_ driver.Pinger             = &conn{}
	_ driver.NamedValueChecker  = &conn{}
)

func (c *conn) Commit() (err error) {
	onDone := trace.SQLOnTxCommit(c.trace)
	defer func() {
		onDone(err)
	}()
	if c.isClosed() {
		return errClosedConn
	}
	defer func() {
		c.transaction = nil
	}()
	_, err = c.transaction.CommitTx(context.Background())
	if err != nil {
		return c.checkClosed(err)
	}
	return nil
}

func (c *conn) Rollback() (err error) {
	onDone := trace.SQLOnTxRollback(c.trace)
	defer func() {
		onDone(err)
	}()
	if c.isClosed() {
		return errClosedConn
	}
	defer func() {
		c.transaction = nil
	}()
	if c.transaction == nil {
		return nil
	}
	err = c.transaction.Rollback(context.Background())
	if err != nil {
		return c.checkClosed(err)
	}
	return err
}

func newConn(c *Connector, s table.ClosableSession, opts ...connOption) *conn {
	cc := &conn{
		connector: c,
		session:   s,
	}
	for _, o := range opts {
		o(cc)
	}
	return cc
}

func (c *conn) checkClosed(err error) error {
	if c.isClosed() {
		return errClosedConn
	}
	if err == nil {
		return nil
	}
	if err = badconn.Map(err); xerrors.Is(err, driver.ErrBadConn) {
		c.setClosed()
	}
	return err
}

func (c *conn) isClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

func (c *conn) setClosed() {
	if atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		c.Close()
	}
}

func (c *conn) PrepareContext(ctx context.Context, query string) (_ driver.Stmt, err error) {
	onDone := trace.SQLOnConnPrepare(c.trace, &ctx, query)
	defer func() {
		onDone(err)
	}()
	if c.isClosed() {
		return nil, errClosedConn
	}
	var s table.Statement
	s, err = c.session.Prepare(ctx, query)
	if err != nil {
		return nil, c.checkClosed(err)
	}
	return &stmt{
		conn:   c,
		params: internal.Params(s),
		query:  query,
		trace:  c.trace,
	}, nil
}

func (c *conn) BeginTx(ctx context.Context, txOptions driver.TxOptions) (_ driver.Tx, err error) {
	onDone := trace.SQLOnConnBeginTx(c.trace, &ctx)
	defer func() {
		onDone(err)
	}()
	if c.isClosed() {
		return nil, errClosedConn
	}
	if c.transaction != nil {
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("conn already have an opened tx: %s", c.transaction.ID()),
		)
	}
	var txSettings *table.TransactionSettings
	txSettings, err = isolation.ToYDB(txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	c.transaction, err = c.session.BeginTransaction(ctx, txSettings)
	if err != nil {
		return nil, c.checkClosed(err)
	}
	return c, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, err error) {
	onDone := trace.SQLOnConnExecContext(c.trace, &ctx, query)
	defer func() {
		onDone(err)
	}()
	if c.isClosed() {
		return nil, errClosedConn
	}
	m := queryModeFromContext(ctx, c.defaultQueryMode)
	if c.transaction != nil {
		if m != DataQueryMode {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("query mode `%s` not supported with transaction", m.String()),
			)
		}
		_, err = c.transaction.Execute(ctx, query, toQueryParams(args), dataQueryOptions(ctx)...)
		if err != nil {
			return nil, c.checkClosed(err)
		}
		return c, nil
	}
	switch m {
	case DataQueryMode:
		var res result.Result
		_, res, err = c.session.Execute(ctx,
			txControl(ctx, c.defaultTxControl),
			query,
			toQueryParams(args),
			dataQueryOptions(ctx)...,
		)
		if err != nil {
			return nil, c.checkClosed(err)
		}
		if err = res.Err(); err != nil {
			return nil, c.checkClosed(err)
		}
		return c, nil
	case SchemeQueryMode:
		err = c.session.ExecuteSchemeQuery(ctx, query)
		if err != nil {
			return nil, c.checkClosed(err)
		}
		return c, nil
	case ScriptingQueryMode:
		_, err = c.connector.connection.Scripting().StreamExecute(ctx, query, toQueryParams(args))
		if err != nil {
			return nil, c.checkClosed(err)
		}
		return c, nil
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute query", m)
	}
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, err error) {
	onDone := trace.SQLOnConnExecContext(c.trace, &ctx, query)
	defer func() {
		onDone(err)
	}()
	if c.isClosed() {
		return nil, errClosedConn
	}
	m := queryModeFromContext(ctx, c.defaultQueryMode)
	if c.transaction != nil {
		if m != DataQueryMode {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("query mode `%s` not supported with transaction", m.String()),
			)
		}
		var res result.Result
		res, err = c.transaction.Execute(ctx, query, toQueryParams(args), dataQueryOptions(ctx)...)
		if err != nil {
			return nil, c.checkClosed(err)
		}
		if err = res.Err(); err != nil {
			return nil, c.checkClosed(err)
		}
		return &rows{
			result: res,
		}, nil
	}
	switch m {
	case DataQueryMode:
		var res result.Result
		_, res, err = c.session.Execute(ctx,
			txControl(ctx, c.defaultTxControl),
			query,
			toQueryParams(args),
			dataQueryOptions(ctx)...,
		)
		if err != nil {
			return nil, c.checkClosed(err)
		}
		if err = res.Err(); err != nil {
			return nil, c.checkClosed(err)
		}
		return &rows{
			result: res,
		}, nil
	case ScanQueryMode:
		var res result.StreamResult
		res, err = c.session.StreamExecuteScanQuery(ctx,
			query,
			toQueryParams(args),
			scanQueryOptions(ctx)...,
		)
		if err != nil {
			return nil, c.checkClosed(err)
		}
		if err = res.Err(); err != nil {
			return nil, c.checkClosed(err)
		}
		return &rows{
			result: res,
		}, nil
	case ExplainQueryMode:
		var exp table.DataQueryExplanation
		exp, err = c.session.Explain(ctx, query)
		if err != nil {
			return nil, c.checkClosed(err)
		}
		return &single{
			values: []sql.NamedArg{
				sql.Named("AST", exp.AST),
				sql.Named("Plan", exp.Plan),
			},
		}, nil
	case ScriptingQueryMode:
		var res result.StreamResult
		res, err = c.connector.connection.Scripting().StreamExecute(ctx, query, toQueryParams(args))
		if err != nil {
			return nil, c.checkClosed(err)
		}
		if err = res.Err(); err != nil {
			return nil, c.checkClosed(err)
		}
		return &rows{
			result: res,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' on conn query", m)
	}
}

func (c *conn) Ping(ctx context.Context) (err error) {
	onDone := trace.SQLOnConnPing(c.trace, &ctx)
	defer func() {
		onDone(err)
	}()
	if c.isClosed() {
		return errClosedConn
	}
	if err = c.session.KeepAlive(ctx); err != nil {
		return c.checkClosed(c.session.KeepAlive(ctx))
	}
	return nil
}

func (c *conn) Close() (err error) {
	onDone := trace.SQLOnConnClose(c.trace)
	defer func() {
		onDone(err)
	}()
	err = c.session.Close(context.Background())
	if err != nil {
		return c.checkClosed(err)
	}
	return nil
}

func (c *conn) Prepare(string) (driver.Stmt, error) {
	return nil, errDeprecated
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errDeprecated
}

func Unwrap(db *sql.DB) (connector *Connector, err error) {
	// hop with create session (connector.Connect()) helps to get ydb.Connection
	c, err := db.Conn(context.Background())
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if err = c.Raw(func(driverConn interface{}) error {
		if cc, ok := driverConn.(*conn); ok {
			connector = cc.connector
			return nil
		}
		return xerrors.WithStackTrace(badconn.Map(fmt.Errorf("%+v is not a *conn", driverConn)))
	}); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return connector, nil
}
