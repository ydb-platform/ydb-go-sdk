package xsql

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/gtrace"
)

type Conn struct {
	processor Engine

	cc        common.Conn
	currentTx *Tx
	ctx       context.Context //nolint:containedctx

	connector *Connector
}

func (c *Conn) ID() string {
	return c.cc.ID()
}

func (c *Conn) NodeID() uint32 {
	return c.cc.NodeID()
}

func (c *Conn) Ping(ctx context.Context) (finalErr error) {
	onDone := gtrace.DatabaseSQLOnConnPing(c.connector.trace, &c.ctx,
		stack.FunctionID("database/sql.(*Conn).Ping" /*stack.Package("database/sql")*/),
	)
	defer func() {
		onDone(finalErr)
	}()

	if err := c.cc.Ping(ctx); err != nil {
		return xerrors.WithStackTrace(badconn.Map(err))
	}

	return nil
}

func (c *Conn) CheckNamedValue(value *driver.NamedValue) (finalErr error) {
	onDone := gtrace.DatabaseSQLOnConnCheckNamedValue(c.connector.trace, &c.ctx,
		stack.FunctionID("database/sql.(*Conn).CheckNamedValue" /*stack.Package("database/sql")*/),
		value,
	)
	defer func() {
		onDone(finalErr)
	}()

	// on this stage allows all values
	return nil
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (_ driver.Tx, finalErr error) {
	onDone := gtrace.DatabaseSQLOnConnBeginTx(c.connector.trace, &ctx,
		stack.FunctionID("database/sql.(*Conn).BeginTx" /*stack.Package("database/sql")*/),
	)
	defer func() {
		if c.currentTx != nil {
			onDone(c.currentTx, finalErr)
		} else {
			onDone(nil, finalErr)
		}
	}()

	if c.currentTx != nil {
		return nil, xerrors.WithStackTrace(xerrors.AlreadyHasTx(c.currentTx.ID()))
	}

	tx, err := c.cc.BeginTx(ctx, opts)
	if err != nil {
		return nil, xerrors.WithStackTrace(badconn.Map(err))
	}

	c.currentTx = &Tx{
		conn: c,
		ctx:  ctx,
		tx:   tx,
	}

	return c.currentTx, nil
}

func (c *Conn) Close() (finalErr error) {
	onDone := gtrace.DatabaseSQLOnConnClose(c.connector.Trace(), &c.ctx,
		stack.FunctionID("database/sql.(*Conn).Close" /*stack.Package("database/sql")*/),
	)
	defer func() {
		onDone(finalErr)
	}()

	err := c.cc.Close(c.ctx)
	if err != nil {
		return xerrors.WithStackTrace(badconn.Map(err))
	}

	return nil
}

// IsValid implements driver.Validator interface.
// database/sql calls IsValid before reusing a connection from the pool.
// If IsValid returns false, the connection is discarded and a new one is requested.
func (c *Conn) IsValid() bool {
	return c.cc.IsValid()
}

func (c *Conn) Begin() (_ driver.Tx, finalErr error) {
	onDone := gtrace.DatabaseSQLOnConnBegin(c.connector.trace, &c.ctx,
		stack.FunctionID("database/sql.(*Conn).Begin" /*stack.Package("database/sql")*/),
	)
	defer func() {
		if c.currentTx != nil {
			onDone(c.currentTx, finalErr)
		} else {
			onDone(nil, finalErr)
		}
	}()

	if c.currentTx != nil {
		return nil, xerrors.WithStackTrace(xerrors.AlreadyHasTx(c.currentTx.ID()))
	}

	return nil, xerrors.WithStackTrace(errDeprecated)
}

func (c *Conn) Prepare(string) (driver.Stmt, error) {
	return nil, errDeprecated
}

func (c *Conn) PrepareContext(ctx context.Context, sql string) (_ driver.Stmt, finalErr error) {
	onDone := gtrace.DatabaseSQLOnConnPrepare(c.connector.Trace(), &ctx,
		stack.FunctionID("database/sql.(*Conn).PrepareContext" /*stack.Package("database/sql")*/),
		sql,
	)
	defer func() {
		onDone(finalErr)
	}()

	if !c.cc.IsValid() {
		return nil, xerrors.WithStackTrace(xerrors.Retryable(errNotReadyConn,
			xerrors.Invalid(c),
			xerrors.Invalid(c.cc),
		))
	}

	return &Stmt{
		conn:      c,
		processor: c.cc,
		ctx:       ctx,
		sql:       sql,
	}, nil
}

func (c *Conn) QueryContext(ctx context.Context, sql string, args []driver.NamedValue) (
	_ driver.Rows, finalErr error,
) {
	onDone := gtrace.DatabaseSQLOnConnQuery(c.connector.Trace(), &ctx,
		stack.FunctionID("database/sql.(*Conn).QueryContext" /*stack.Package("database/sql")*/),
		sql, c.connector.processor.String(), xcontext.IsIdempotent(ctx),
	)
	defer func() {
		onDone(finalErr)
	}()

	sql, params, err := c.toYdb(sql, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if isExplain(ctx) {
		ast, plan, err := c.cc.Explain(ctx, sql, params)
		if err != nil {
			return nil, xerrors.WithStackTrace(badconn.Map(err))
		}

		return newRows(ctx, rowByAstPlan(ast, plan)), nil
	}

	if c.currentTx != nil {
		rows, err := c.currentTx.tx.Query(ctx, sql, params)
		if err != nil {
			return nil, xerrors.WithStackTrace(badconn.Map(err))
		}

		return newRows(ctx, rows), nil
	}

	result, err := c.cc.Query(ctx, sql, params)
	if err != nil {
		return nil, xerrors.WithStackTrace(badconn.Map(err))
	}

	return newRows(ctx, result), nil
}

func (c *Conn) ExecContext(ctx context.Context, sql string, args []driver.NamedValue) (
	_ driver.Result, finalErr error,
) {
	onDone := gtrace.DatabaseSQLOnConnExec(c.connector.Trace(), &ctx,
		stack.FunctionID("database/sql.(*Conn).ExecContext" /*stack.Package("database/sql")*/),
		sql, c.connector.processor.String(), xcontext.IsIdempotent(ctx),
	)
	defer func() {
		onDone(finalErr)
	}()

	sql, params, err := c.toYdb(sql, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if c.currentTx != nil {
		result, err := c.currentTx.tx.Exec(ctx, sql, params)
		if err != nil {
			return nil, xerrors.WithStackTrace(badconn.Map(err))
		}

		return result, nil
	}

	result, err := c.cc.Exec(ctx, sql, params)
	if err != nil {
		return nil, xerrors.WithStackTrace(badconn.Map(err))
	}

	return result, nil
}
