package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
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

func (r *singleRow) Columns(_ context.Context) (columns []string) {
	for i := range r.values {
		columns = append(columns, r.values[i].Name)
	}

	return columns
}

func (r *singleRow) Close(context.Context) error {
	return nil
}

func (r *singleRow) ColumnTypeDatabaseTypeName(_ context.Context, _ int) string {
	return utf8DatabaseTypeName
}

func (r *singleRow) ColumnTypeNullable(_ context.Context, _ int) (nullable, ok bool) {
	return false, true
}

func (r *singleRow) HasNextResultSet(_ context.Context) bool {
	return false
}

func (r *singleRow) NextResultSet(_ context.Context) error {
	return io.EOF
}

func (r *singleRow) Next(_ context.Context, dst []driver.Value) error {
	if r.values == nil || r.readAll {
		return io.EOF
	}
	for i := range r.values {
		dst[i] = r.values[i].Value
	}
	r.readAll = true

	return nil
}

// Rows wraps internal common.Rows using the pinned database/sql QueryContext so
// driver.Rows.Next / RowsNextResultSet stay context-aware without storing the
// context on xquery or xtable row implementations.
type Rows struct {
	queryCtx context.Context //nolint:containedctx
	inner    common.Rows
}

var utf8DatabaseTypeName = types.TypeText.String()

var (
	_ driver.RowsNextResultSet              = (*Rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*Rows)(nil)
	_ driver.RowsColumnTypeNullable         = (*Rows)(nil)
)

func newRows(queryCtx context.Context, rows common.Rows) driver.RowsNextResultSet {
	if rows == nil {
		return nil
	}

	return &Rows{
		queryCtx: queryCtx,
		inner:    rows,
	}
}

func (r *Rows) Columns() []string {
	return r.inner.Columns(r.queryCtx)
}

func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.inner.ColumnTypeDatabaseTypeName(r.queryCtx, index)
}

func (r *Rows) ColumnTypeNullable(index int) (nullable bool, ok bool) {
	return r.inner.ColumnTypeNullable(r.queryCtx, index)
}

func (r *Rows) HasNextResultSet() bool {
	return r.inner.HasNextResultSet(r.queryCtx)
}

func (r *Rows) Next(dst []driver.Value) error {
	if err := r.inner.Next(r.queryCtx, dst); err != nil {
		if xerrors.Is(err, io.EOF) {
			return io.EOF
		}

		return xerrors.WithStackTrace(badconn.Map(err))
	}

	return nil
}

func (r *Rows) NextResultSet() error {
	if err := r.inner.NextResultSet(r.queryCtx); err != nil {
		if xerrors.Is(err, io.EOF) {
			return io.EOF
		}

		return xerrors.WithStackTrace(badconn.Map(err))
	}

	return nil
}

func (r *Rows) Close() error {
	if err := r.inner.Close(r.queryCtx); err != nil {
		return xerrors.WithStackTrace(badconn.Map(err))
	}

	return nil
}
