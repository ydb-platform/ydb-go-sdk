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
	query   string
	stmtCtx context.Context

	trace *trace.DatabaseSQL
}

var (
	_ driver.Stmt             = &stmt{}
	_ driver.StmtQueryContext = &stmt{}
	_ driver.StmtExecContext  = &stmt{}
)

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (_ driver.Rows, finalErr error) {
	onDone := trace.DatabaseSQLOnStmtQuery(s.trace, &ctx,
		stack.FunctionID(""),
		s.stmtCtx, s.query,
	)
	defer func() {
		onDone(finalErr)
	}()
	if !s.conn.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	switch m := queryModeFromContext(ctx, s.conn.defaultQueryMode); m {
	case DataQueryMode:
		return s.processor.QueryContext(s.conn.withKeepInCache(ctx), s.query, args)
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute query on prepared statement", m)
	}
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (_ driver.Result, finalErr error) {
	onDone := trace.DatabaseSQLOnStmtExec(s.trace, &ctx,
		stack.FunctionID(""),
		s.stmtCtx, s.query,
	)
	defer func() {
		onDone(finalErr)
	}()
	if !s.conn.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	switch m := queryModeFromContext(ctx, s.conn.defaultQueryMode); m {
	case DataQueryMode:
		return s.processor.ExecContext(s.conn.withKeepInCache(ctx), s.query, args)
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute query on prepared statement", m)
	}
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Close() (finalErr error) {
	onDone := trace.DatabaseSQLOnStmtClose(s.trace, &s.stmtCtx, stack.FunctionID(""))
	defer func() {
		onDone(finalErr)
	}()
	return nil
}

func (s *stmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errDeprecated
}

func (s *stmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errDeprecated
}
