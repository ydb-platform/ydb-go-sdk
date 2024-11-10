package conn

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ driver.Conn               = &Conn{}
	_ driver.ConnPrepareContext = &Conn{}
	_ driver.ConnBeginTx        = &Conn{}
	_ driver.ExecerContext      = &Conn{}
	_ driver.QueryerContext     = &Conn{}
	_ driver.Pinger             = &Conn{}
	_ driver.Validator          = &Conn{}
	_ driver.NamedValueChecker  = &Conn{}
)

func (c *Conn) ID() string {
	return c.session.ID()
}

func (c *Conn) IsValid() bool {
	return c.isReady()
}

func (c *Conn) CheckNamedValue(value *driver.NamedValue) error {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) Ping(ctx context.Context) (finalErr error) {
	onDone := trace.DatabaseSQLOnConnPing(c.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn.(*Conn).Ping"),
	)
	defer func() {
		onDone(finalErr)
	}()

	if !c.isReady() {
		return badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	if !c.session.Core.IsAlive() {
		return badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	err := c.session.Exec(ctx, "select 1")

	return err
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !c.IsValid() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	// TODO TX
	// if c.currentTx != nil {
	// 	return c.currentTx.ExecContext(ctx, query, args)
	// }

	return c.execContext(ctx, query, args)
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	//TODO implement me
	panic("implement me")
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
		onDone = trace.DatabaseSQLOnConnClose(
			c.parent.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn.(*Conn).Close"),
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

func (c *Conn) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) LastUsage() time.Time {
	//TODO implement me
	panic("implement me")
}
