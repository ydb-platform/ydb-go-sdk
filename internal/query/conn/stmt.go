package conn

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type stmt struct {
	conn      *Conn
	processor interface {
		driver.ExecerContext
		driver.QueryerContext
	}
	query string
	ctx   context.Context //nolint:containedctx
}

var (
	_ driver.Stmt             = &stmt{}
	_ driver.StmtQueryContext = &stmt{}
	_ driver.StmtExecContext  = &stmt{}
)

func (stmt *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (_ driver.Rows, finalErr error) {
	onDone := trace.DatabaseSQLOnStmtQuery(stmt.conn.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn.(*stmt).QueryContext"),
		stmt.ctx, stmt.query,
	)
	defer func() {
		onDone(finalErr)
	}()
	if !stmt.conn.isReady() {
		return nil, xerrors.WithStackTrace(errNotReadyConn)
	}

	return stmt.processor.QueryContext(ctx, stmt.query, args)
}

func (stmt *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (_ driver.Result, finalErr error) {
	onDone := trace.DatabaseSQLOnStmtExec(stmt.conn.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn.(*stmt).ExecContext"),
		stmt.ctx, stmt.query,
	)
	defer func() {
		onDone(finalErr)
	}()
	if !stmt.conn.isReady() {
		return nil, xerrors.WithStackTrace(errNotReadyConn)
	}

	return stmt.processor.ExecContext(ctx, stmt.query, args)
}

func (stmt *stmt) NumInput() int {
	return -1
}

func (stmt *stmt) Close() (finalErr error) {
	var (
		ctx    = stmt.ctx
		onDone = trace.DatabaseSQLOnStmtClose(stmt.conn.parent.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn.(*stmt).Close"),
		)
	)
	defer func() {
		onDone(finalErr)
	}()

	return nil
}

func (stmt *stmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errDeprecated
}

func (stmt *stmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errDeprecated
}
