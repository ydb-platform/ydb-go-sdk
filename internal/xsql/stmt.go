package xsql

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type stmt struct {
	conn      *conn
	processor interface {
		driver.ExecerContext
		driver.QueryerContext
	}
	query string
	ctx   context.Context //nolint:containedctx

	trace *trace.DatabaseSQL
}

var (
	_ driver.Stmt             = &stmt{}
	_ driver.StmtQueryContext = &stmt{}
	_ driver.StmtExecContext  = &stmt{}
)

func (stmt *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (_ driver.Rows, finalErr error) {
	onDone := trace.DatabaseSQLOnStmtQuery(stmt.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/xsql.(*stmt).QueryContext"),
		stmt.ctx, stmt.query,
	)
	defer func() {
		onDone(finalErr)
	}()
	if !stmt.conn.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	switch m := queryModeFromContext(ctx, stmt.conn.defaultQueryMode); m {
	case DataQueryMode:
		return stmt.processor.QueryContext(stmt.conn.withKeepInCache(ctx), stmt.query, args)
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute query on prepared statement", m)
	}
}

func (stmt *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (_ driver.Result, finalErr error) {
	onDone := trace.DatabaseSQLOnStmtExec(stmt.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/xsql.(*stmt).ExecContext"),
		stmt.ctx, stmt.query,
	)
	defer func() {
		onDone(finalErr)
	}()
	if !stmt.conn.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	switch m := queryModeFromContext(ctx, stmt.conn.defaultQueryMode); m {
	case DataQueryMode:
		return stmt.processor.ExecContext(stmt.conn.withKeepInCache(ctx), stmt.query, args)
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute query on prepared statement", m)
	}
}

func (stmt *stmt) NumInput() int {
	return -1
}

func (stmt *stmt) Close() (finalErr error) {
	var (
		ctx    = stmt.ctx
		onDone = trace.DatabaseSQLOnStmtClose(stmt.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/xsql.(*stmt).Close"),
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
