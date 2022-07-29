package tx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/ydb-platform/ydb-go-sql/internal/xerrors"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/ydb-platform/ydb-go-sql/internal/mode"
	"github.com/ydb-platform/ydb-go-sql/internal/rows"
	"github.com/ydb-platform/ydb-go-sql/internal/single"
	"github.com/ydb-platform/ydb-go-sql/internal/x"
)

type ro struct {
	s     table.ClosableSession
	txc   *table.TransactionControl
	close func()
}

func (tx *ro) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch m := x.QueryMode(ctx); m {
	case mode.DataQuery:
		_, res, err := tx.s.Execute(ctx, tx.txc, query, x.ToQueryParams(args))
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
		return nil, fmt.Errorf("unsupported query mode %s types on ro tx query", m)
	}
}

func (tx *ro) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return nil, xerrors.ErrExecOnReadOnlyTx
}

func (tx *ro) Commit() error {
	defer tx.close()
	return nil
}

func (tx *ro) Rollback() error {
	defer tx.close()
	return nil
}
