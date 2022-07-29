package stmt

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/ydb-platform/ydb-go-sql/internal/xerrors"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/ydb-platform/ydb-go-sql/internal/check"
	"github.com/ydb-platform/ydb-go-sql/internal/mode"
	"github.com/ydb-platform/ydb-go-sql/internal/nop"
	"github.com/ydb-platform/ydb-go-sql/internal/rows"
	"github.com/ydb-platform/ydb-go-sql/internal/x"
)

type Stmt interface {
	driver.Stmt
	driver.StmtQueryContext
	driver.StmtExecContext

	driver.NamedValueChecker
}

type stmt struct {
	stmt             table.Statement
	defaultTxControl *table.TransactionControl
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	switch m := x.QueryMode(ctx); m {
	case mode.DataQuery:
		_, res, err := s.stmt.Execute(ctx, x.TxControl(ctx, s.defaultTxControl), x.ToQueryParams(args), x.DataQueryOptions(ctx)...)
		if err != nil {
			return nil, xerrors.Map(err)
		}
		return rows.Result(res), xerrors.Map(res.Err())
	default:
		return nil, fmt.Errorf("unsupported query mode %s types for execute statement query", m)
	}
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	switch m := x.QueryMode(ctx); m {
	case mode.DataQuery:
		_, res, err := s.stmt.Execute(ctx, x.TxControl(ctx, s.defaultTxControl), x.ToQueryParams(args), x.DataQueryOptions(ctx)...)
		if err != nil {
			return nil, xerrors.Map(err)
		}
		return nop.Result(), xerrors.Map(res.Err())
	default:
		return nil, fmt.Errorf("unsupported query mode %s types for execute query", m)
	}
}

func New(
	s table.Statement,
	defaultTxControl *table.TransactionControl,
) Stmt {
	return &stmt{
		stmt:             s,
		defaultTxControl: defaultTxControl,
	}
}

func (s *stmt) NumInput() int {
	return s.stmt.NumInput()
}

func (s *stmt) Close() error {
	return nil
}

func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, xerrors.ErrDeprecated
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, xerrors.ErrDeprecated
}

func (s *stmt) CheckNamedValue(v *driver.NamedValue) error {
	return check.NamedValue(v)
}
