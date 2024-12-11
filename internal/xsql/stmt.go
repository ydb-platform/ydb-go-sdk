package xsql

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type stmt struct {
	conn      *connWrapper
	processor interface {
		Exec(ctx context.Context, sql string, params *params.Params) (driver.Result, error)
		Query(ctx context.Context, sql string, params *params.Params) (driver.RowsNextResultSet, error)
	}
	sql string
	ctx context.Context //nolint:containedctx
}

var (
	_ driver.Stmt             = &stmt{}
	_ driver.StmtQueryContext = &stmt{}
	_ driver.StmtExecContext  = &stmt{}
)

func (stmt *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (_ driver.Rows, finalErr error) {
	onDone := trace.DatabaseSQLOnStmtQuery(stmt.conn.connector.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql.(*stmt).QueryContext"),
		stmt.ctx, stmt.sql,
	)
	defer func() {
		onDone(finalErr)
	}()

	if !stmt.conn.cc.IsValid() {
		return nil, xerrors.WithStackTrace(errNotReadyConn)
	}

	sql, params, err := stmt.conn.normalize(stmt.sql, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return stmt.processor.Query(ctx, sql, params)
}

func (stmt *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (_ driver.Result, finalErr error) {
	onDone := trace.DatabaseSQLOnStmtExec(stmt.conn.connector.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql.(*stmt).ExecContext"),
		stmt.ctx, stmt.sql,
	)
	defer func() {
		onDone(finalErr)
	}()

	if !stmt.conn.cc.IsValid() {
		return nil, xerrors.WithStackTrace(errNotReadyConn)
	}

	sql, params, err := stmt.conn.normalize(stmt.sql, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return stmt.processor.Exec(ctx, sql, params)
}

func (stmt *stmt) NumInput() int {
	return -1
}

func (stmt *stmt) Close() (finalErr error) {
	var (
		ctx    = stmt.ctx
		onDone = trace.DatabaseSQLOnStmtClose(stmt.conn.connector.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql.(*stmt).Close"),
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
