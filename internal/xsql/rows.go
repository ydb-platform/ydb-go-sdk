package xsql

import (
	"database/sql"
	"database/sql/driver"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
)

type singleRow struct {
	values  []sql.NamedArg
	readAll bool
}

func rowByAstPlan(ast, plan string) *singleRow {
	return &singleRow{
		values: []sql.NamedArg{
			{
				Name:  "Ast",
				Value: ast,
			},
			{
				Name:  "Plan",
				Value: plan,
			},
		},
	}
}

func (r *singleRow) Columns() (columns []string) {
	for i := range r.values {
		columns = append(columns, r.values[i].Name)
	}

	return columns
}

func (r *singleRow) Close() error {
	return nil
}

func (r *singleRow) Next(dst []driver.Value) error {
	if r.values == nil || r.readAll {
		return io.EOF
	}
	for i := range r.values {
		dst[i] = r.values[i].Value
	}
	r.readAll = true

	return nil
}

// Rows wraps driver.RowsNextResultSet and applies badconn.Map to errors
// returned by Next and NextResultSet so that session-invalidating errors
// during row iteration are correctly signaled to database/sql.
type Rows struct {
	common.Rows
}

func newRows(rows common.Rows) driver.RowsNextResultSet {
	if rows == nil {
		return nil
	}

	return &Rows{Rows: rows}
}

func (r *Rows) Next(dst []driver.Value) error {
	if err := r.Rows.Next(dst); err != nil {
		if xerrors.Is(err, io.EOF) {
			return io.EOF
		}
		return xerrors.WithStackTrace(badconn.Map(err))
	}

	return nil
}

func (r *Rows) NextResultSet() error {
	if err := r.Rows.NextResultSet(); err != nil {
		if xerrors.Is(err, io.EOF) {
			return io.EOF
		}
		return xerrors.WithStackTrace(badconn.Map(err))
	}

	return nil
}

func (r *Rows) Close() error {
	if err := r.Rows.Close(); err != nil {
		return xerrors.WithStackTrace(badconn.Map(err))
	}

	return nil
}
