package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync/atomic"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/isolation"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
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

	currentTx *tx
}

var (
	_ driver.Conn               = &conn{}
	_ driver.ConnPrepareContext = &conn{}
	_ driver.ConnBeginTx        = &conn{}
	_ driver.ExecerContext      = &conn{}
	_ driver.QueryerContext     = &conn{}
	_ driver.SessionResetter    = &conn{}
	_ driver.Pinger             = &conn{}
	_ driver.NamedValueChecker  = &conn{}
)

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
	if retry.Check(err).MustDeleteSession() {
		c.setClosed()
		return badConn(err)
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

func (c *conn) ResetSession(ctx context.Context) error {
	c.setClosed()
	return errClosedConn
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.isClosed() {
		return nil, errClosedConn
	}
	s, err := c.session.Prepare(ctx, query)
	if err != nil {
		return nil, c.checkClosed(err)
	}
	return &stmt{
		conn:   c,
		params: internal.Params(s),
		query:  query,
	}, nil
}

func (c *conn) BeginTx(ctx context.Context, txOptions driver.TxOptions) (driver.Tx, error) {
	if c.isClosed() {
		return nil, errClosedConn
	}
	if c.currentTx != nil {
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("conn already have an opened tx: %s", c.currentTx.transaction.ID()),
		)
	}
	txSettings, err := isolation.ToYDB(txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	t, err := c.session.BeginTransaction(ctx, txSettings)
	if err != nil {
		return nil, c.checkClosed(err)
	}
	c.currentTx = &tx{
		conn:        c,
		transaction: t,
	}
	return c.currentTx, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.isClosed() {
		return nil, errClosedConn
	}
	m := queryModeFromContext(ctx, c.defaultQueryMode)
	if c.currentTx != nil && m == DataQueryMode {
		return c.currentTx.ExecContext(ctx, query, args)
	}
	switch m {
	case DataQueryMode:
		_, res, err := c.session.Execute(ctx, txControl(ctx, c.defaultTxControl), query, toQueryParams(args))
		if err != nil {
			return nil, c.checkClosed(err)
		}
		if err = res.Err(); err != nil {
			return nil, c.checkClosed(err)
		}
		return c, nil
	case SchemeQueryMode:
		err := c.session.ExecuteSchemeQuery(ctx, query, toSchemeOptions(args)...)
		if err != nil {
			return nil, c.checkClosed(err)
		}
		return c, nil
	case ScriptingQueryMode:
		_, err := c.connector.connection.Scripting().StreamExecute(ctx, query, toQueryParams(args))
		if err != nil {
			return nil, c.checkClosed(err)
		}
		return c, nil
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute query", m)
	}
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.isClosed() {
		return nil, errClosedConn
	}
	m := queryModeFromContext(ctx, c.defaultQueryMode)
	if c.currentTx != nil && m == DataQueryMode {
		return c.currentTx.QueryContext(ctx, query, args)
	}
	switch m {
	case DataQueryMode:
		_, res, err := c.session.Execute(ctx, txControl(ctx, c.defaultTxControl), query, toQueryParams(args))
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
		res, err := c.session.StreamExecuteScanQuery(ctx, query, toQueryParams(args), scanQueryOptions(ctx)...)
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
		exp, err := c.session.Explain(ctx, query)
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
		res, err := c.connector.connection.Scripting().StreamExecute(ctx, query, toQueryParams(args))
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
	if c.isClosed() {
		return errClosedConn
	}
	if err = c.session.KeepAlive(ctx); err != nil {
		return c.checkClosed(c.session.KeepAlive(ctx))
	}
	return nil
}

func (c *conn) Close() error {
	ctx := context.Background()
	err := c.session.Close(ctx)
	return c.checkClosed(err)
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
		return xerrors.WithStackTrace(badConn(fmt.Errorf("%+v is not a *conn", driverConn)))
	}); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return connector, nil
}
