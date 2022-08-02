package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/stmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/tx"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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
	parent *Connector
	s      table.ClosableSession // Immutable and r/o usage.
	closed uint32

	defaultQueryMode QueryMode
	defaultTxControl *table.TransactionControl

	dataOpts []options.ExecuteDataQueryOption
	scanOpts []options.ExecuteScanQueryOption
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

func newConn(parent *Connector, s table.ClosableSession, opts ...connOption) *conn {
	c := &conn{
		parent: parent,
		s:      s,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *conn) isClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

func (c *conn) setClosed() {
	atomic.StoreUint32(&c.closed, 1)
}

func (c *conn) ResetSession(ctx context.Context) error {
	return BadConn(ErrUnsupported)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	s, err := c.s.Prepare(ctx, query)
	if err != nil {
		return nil, BadConn(err)
	}
	return stmt.New(s, c.defaultTxControl), nil
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := tx.New(ctx, opts, c.s, func() { c.tx = nil })
	return c.tx, err
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.tx != nil {
		return c.tx.ExecContext(ctx, query, args)
	}
	switch m := x.QueryMode(ctx); m {
	case QueryModeData:
		_, res, err := c.s.Execute(ctx, x.TxControl(ctx, c.defaultTxControl), query, x.ToQueryParams(args))
		if err != nil {
			return nil, BadConn(err)
		}
		return nop.Result(), BadConn(res.Err())
	case QueryModeScheme:
		err := c.s.ExecuteSchemeQuery(ctx, query, x.ToSchemeOptions(args)...)
		if err != nil {
			return nil, BadConn(err)
		}
		return nop.Result(), nil
	default:
		return nil, fmt.Errorf("unsupported query mode %s types for execute query", m)
	}
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.tx != nil {
		return c.tx.QueryContext(ctx, query, args)
	}
	switch m := x.QueryMode(ctx); m {
	case QueryModeData:
		_, res, err := c.s.Execute(ctx, x.TxControl(ctx, c.defaultTxControl), query, x.ToQueryParams(args))
		if err != nil {
			return nil, BadConn(err)
		}
		return rows.Result(res), BadConn(res.Err())
	case QueryModeScan:
		res, err := c.s.StreamExecuteScanQuery(ctx, query, x.ToQueryParams(args), x.ScanQueryOptions(ctx)...)
		if err != nil {
			return nil, BadConn(err)
		}
		return stream.Result(ctx, res), BadConn(res.Err())
	case QueryModeExplain:
		exp, err := c.s.Explain(ctx, query)
		if err != nil {
			return nil, BadConn(err)
		}
		return single.Result(
			sql.Named("AST", exp.AST),
			sql.Named("Plan", exp.Plan),
		), nil
	default:
		return nil, fmt.Errorf("unsupported query mode %s types on conn query", m)
	}
}

func (c *conn) CheckNamedValue(v *driver.NamedValue) error {
	return CheckNamedValue(v)
}

func (c *conn) Ping(ctx context.Context) error {
	return BadConn(c.s.KeepAlive(ctx))
}

func (c *conn) Close() error {
	ctx := context.Background()
	err := c.s.Close(ctx)
	return BadConn(err)
}

func (c *conn) Prepare(string) (driver.Stmt, error) {
	return nil, xerrors.ErrDeprecated
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, xerrors.ErrDeprecated
}
