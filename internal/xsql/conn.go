package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
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

func withTrace(t trace.DatabaseSQL) connOption {
	return func(c *conn) {
		c.trace = t
	}
}

type conn struct {
	connector *Connector
	trace     trace.DatabaseSQL
	session   table.ClosableSession // Immutable and r/o usage.

	closed           uint32
	lastUsage        int64
	defaultQueryMode QueryMode

	defaultTxControl *table.TransactionControl
	dataOpts         []options.ExecuteDataQueryOption

	scanOpts []options.ExecuteScanQueryOption

	currentTx currentTx
}

func (c *conn) IsValid() bool {
	return c.isReady()
}

type currentTx interface {
	driver.Tx
	driver.ExecerContext
	driver.QueryerContext
	table.TransactionIdentifier
}

var (
	_ driver.Conn               = &conn{}
	_ driver.ConnPrepareContext = &conn{}
	_ driver.ConnBeginTx        = &conn{}
	_ driver.ExecerContext      = &conn{}
	_ driver.QueryerContext     = &conn{}
	_ driver.Pinger             = &conn{}
	_ driver.NamedValueChecker  = &conn{}
	_ driver.Validator          = &conn{}
)

func newConn(c *Connector, s table.ClosableSession, opts ...connOption) *conn {
	cc := &conn{
		connector: c,
		session:   s,
	}
	for _, o := range opts {
		if o != nil {
			o(cc)
		}
	}
	c.attach(cc)
	return cc
}

func (c *conn) isReady() bool {
	return c.session.Status() == table.SessionReady
}

func (conn) CheckNamedValue(v *driver.NamedValue) (err error) {
	return checkNamedValue(v)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (_ driver.Stmt, err error) {
	onDone := trace.DatabaseSQLOnConnPrepare(c.trace, &ctx, query)
	defer func() {
		onDone(err)
	}()
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	return &stmt{
		conn:  c,
		query: query,
		trace: c.trace,
	}, nil
}

func (c *conn) sinceLastUsage() time.Duration {
	return time.Since(time.Unix(atomic.LoadInt64(&c.lastUsage), 0))
}

func (c *conn) execContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, err error) {
	m := queryModeFromContext(ctx, c.defaultQueryMode)
	onDone := trace.DatabaseSQLOnConnExec(
		c.trace,
		&ctx,
		query,
		m.String(),
		xcontext.IsIdempotent(ctx),
		c.sinceLastUsage(),
	)
	defer func() {
		atomic.StoreInt64(&c.lastUsage, time.Now().Unix())
		onDone(err)
	}()
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
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		defer func() {
			_ = res.Close()
		}()
		if err = res.NextResultSetErr(ctx); !xerrors.Is(err, nil, io.EOF) {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		if err = res.Err(); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return driver.ResultNoRows, nil
	case SchemeQueryMode:
		err = c.session.ExecuteSchemeQuery(ctx, query)
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return driver.ResultNoRows, nil
	case ScriptingQueryMode:
		var res result.StreamResult
		res, err = c.connector.connection.Scripting().StreamExecute(ctx, query, toQueryParams(args))
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		defer func() {
			_ = res.Close()
		}()
		if err = res.NextResultSetErr(ctx); !xerrors.Is(err, nil, io.EOF) {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		if err = res.Err(); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return driver.ResultNoRows, nil
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute query", m)
	}
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, err error) {
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return c.currentTx.ExecContext(ctx, query, args)
	}
	return c.execContext(ctx, query, args)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, err error) {
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return c.currentTx.QueryContext(ctx, query, args)
	}
	return c.queryContext(ctx, query, args)
}

func (c *conn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, err error) {
	m := queryModeFromContext(ctx, c.defaultQueryMode)
	onDone := trace.DatabaseSQLOnConnQuery(
		c.trace,
		&ctx,
		query,
		m.String(),
		xcontext.IsIdempotent(ctx),
		c.sinceLastUsage(),
	)
	defer func() {
		atomic.StoreInt64(&c.lastUsage, time.Now().Unix())
		onDone(err)
	}()
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
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		if err = res.Err(); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return &rows{
			conn:   c,
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
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		if err = res.Err(); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return &rows{
			conn:   c,
			result: res,
		}, nil
	case ExplainQueryMode:
		var exp table.DataQueryExplanation
		exp, err = c.session.Explain(ctx, query)
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
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
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		if err = res.Err(); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return &rows{
			conn:   c,
			result: res,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' on conn query", m)
	}
}

func (c *conn) Ping(ctx context.Context) (err error) {
	onDone := trace.DatabaseSQLOnConnPing(c.trace, &ctx)
	defer func() {
		onDone(err)
	}()
	if !c.isReady() {
		return badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if err = c.session.KeepAlive(ctx); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	return nil
}

func (c *conn) Close() (err error) {
	if atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		c.connector.detach(c)
		onDone := trace.DatabaseSQLOnConnClose(c.trace)
		defer func() {
			onDone(err)
		}()
		err = c.session.Close(context.Background())
		if err != nil {
			return badconn.Map(xerrors.WithStackTrace(err))
		}
		return nil
	}
	return badconn.Map(xerrors.WithStackTrace(errConnClosedEarly))
}

func (c *conn) Prepare(string) (driver.Stmt, error) {
	return nil, errDeprecated
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errDeprecated
}

func (c *conn) BeginTx(ctx context.Context, txOptions driver.TxOptions) (_ driver.Tx, err error) {
	var transaction table.Transaction
	onDone := trace.DatabaseSQLOnConnBegin(c.trace, &ctx)
	defer func() {
		onDone(transaction, err)
	}()
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("conn already have an opened currentTx: %s", c.currentTx.ID()),
		)
	}
	var txc table.TxOption
	txc, err = isolation.ToYDB(txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	transaction, err = c.session.BeginTransaction(ctx, table.TxSettings(txc))
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	c.currentTx = &tx{
		conn: c,
		ctx:  ctx,
		tx:   transaction,
	}
	return c.currentTx, nil
}
