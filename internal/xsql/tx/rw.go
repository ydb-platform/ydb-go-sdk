package tx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/ydb-platform/ydb-go-sql/internal/xerrors"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/ydb-platform/ydb-go-sql/internal/mode"
	"github.com/ydb-platform/ydb-go-sql/internal/nop"
	"github.com/ydb-platform/ydb-go-sql/internal/rows"
	"github.com/ydb-platform/ydb-go-sql/internal/single"
	"github.com/ydb-platform/ydb-go-sql/internal/x"
)

type rw struct {
	s     table.ClosableSession
	tx    table.Transaction
	txc   *table.TransactionControl
	close func()
}

func (tx *rw) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch m := x.QueryMode(ctx); m {
	case mode.DataQuery:
		res, err := tx.tx.Execute(ctx, query, x.ToQueryParams(args))
		if err != nil {
			return nil, xerrors.Map(err)
		}
		return rows.Result(res), nil
	case mode.ExplainQuery:
		exp, err := tx.s.Explain(ctx, query)
		if err != nil {
			return nil, xerrors.Map(err)
		}
		return single.Result(
			sql.Named("AST", exp.AST),
			sql.Named("Plan", exp.Plan),
		), nil
	default:
		return nil, fmt.Errorf("unsupported query mode %s types on rw tx query", m)
	}
}

func (tx *rw) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := tx.tx.Execute(ctx, query, x.ToQueryParams(args))
	if err != nil {
		return nil, xerrors.Map(err)
	}
	return nop.Result(), xerrors.Map(res.Err())
}

func (tx *rw) Commit() (err error) {
	_, err = tx.tx.CommitTx(context.Background())
	if err == nil {
		tx.close()
	}
	return err
}

func (tx *rw) Rollback() (err error) {
	err = tx.tx.Rollback(context.Background())
	if err == nil {
		tx.close()
	}
	return err
}
