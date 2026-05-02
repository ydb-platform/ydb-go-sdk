package xsql

import (
	"database/sql"
	"database/sql/driver"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
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

// badconnRows wraps driver.RowsNextResultSet and applies badconn.Map to errors
// returned by Next and NextResultSet so that session-invalidating errors
// during row iteration are correctly signaled to database/sql.
type badconnRows struct {
	driver.RowsNextResultSet
}

func newBadconnRows(rows driver.RowsNextResultSet) driver.RowsNextResultSet {
	if rows == nil {
		return nil
	}

	return &badconnRows{RowsNextResultSet: rows}
}

func (r *badconnRows) Next(dst []driver.Value) error {
	return badconn.Map(r.RowsNextResultSet.Next(dst))
}

func (r *badconnRows) NextResultSet() error {
	return badconn.Map(r.RowsNextResultSet.NextResultSet())
}

// ColumnTypeDatabaseTypeName forwards to the underlying rows if it implements
// driver.RowsColumnTypeDatabaseTypeName.
func (r *badconnRows) ColumnTypeDatabaseTypeName(index int) string {
	if v, ok := r.RowsNextResultSet.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return v.ColumnTypeDatabaseTypeName(index)
	}

	return ""
}

// ColumnTypeNullable forwards to the underlying rows if it implements
// driver.RowsColumnTypeNullable.
func (r *badconnRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if v, ok := r.RowsNextResultSet.(driver.RowsColumnTypeNullable); ok {
		return v.ColumnTypeNullable(index)
	}

	return false, false
}
