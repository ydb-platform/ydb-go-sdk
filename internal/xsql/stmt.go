package xsql

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type stmt struct {
	nopResult
	namedValueChecker

	conn   *conn
	tx     *tx
	params map[string]*Ydb.Type
	query  string
}

var (
	_ driver.Stmt             = &stmt{}
	_ driver.StmtQueryContext = &stmt{}
	_ driver.StmtExecContext  = &stmt{}

	_ driver.NamedValueChecker = &stmt{}
)

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.conn.isClosed() {
		return nil, errClosedConn
	}
	m := queryModeFromContext(ctx, s.conn.defaultQueryMode)
	if s.tx != nil {
		if m != DataQueryMode {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("query mode `%s` not supported with prepared statement", m.String()),
			)
		}
		return s.tx.QueryContext(withKeepInCache(ctx), s.query, args)
	}
	switch m {
	case DataQueryMode:
		_, res, err := s.conn.session.Execute(ctx,
			txControl(ctx, s.conn.defaultTxControl),
			s.query,
			toQueryParams(args),
			dataQueryOptions(withKeepInCache(ctx))...,
		)
		if err != nil {
			return nil, s.conn.checkClosed(err)
		}
		if err = res.Err(); err != nil {
			return nil, s.conn.checkClosed(res.Err())
		}
		return &rows{
			result: res,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute statement query", m)
	}
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.conn.isClosed() {
		return nil, errClosedConn
	}
	m := queryModeFromContext(ctx, s.conn.defaultQueryMode)
	if s.tx != nil {
		if m != DataQueryMode {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("query mode `%s` not supported with prepared statement", m.String()),
			)
		}
		return s.tx.ExecContext(withKeepInCache(ctx), s.query, args)
	}
	switch m {
	case DataQueryMode:
		_, res, err := s.conn.session.Execute(ctx,
			txControl(ctx, s.conn.defaultTxControl),
			s.query,
			toQueryParams(args),
			dataQueryOptions(withKeepInCache(ctx))...,
		)
		if err != nil {
			return nil, s.conn.checkClosed(err)
		}
		if err = res.Err(); err != nil {
			return nil, s.conn.checkClosed(res.Err())
		}
		return s, nil
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute query", m)
	}
}

func (s *stmt) NumInput() int {
	return len(s.params)
}

func (s *stmt) Close() error {
	return nil
}

func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errDeprecated
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errDeprecated
}
