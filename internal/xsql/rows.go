package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	_ driver.Rows                           = &rows{}
	_ driver.RowsNextResultSet              = &rows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &rows{}
	_ driver.Rows                           = &single{}

	_ types.Scanner = &valuer{}
)

type rows struct {
	conn   *conn
	result result.BaseResult

	// nextSet once need for get first result set as default.
	// Iterate over many result sets must be with rows.NextResultSet()
	nextSet sync.Once
}

func (r *rows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (r *rows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

func (r *rows) Columns() []string {
	r.nextSet.Do(func() {
		r.result.NextResultSet(context.Background())
	})

	var i int
	cs := make([]string, r.result.CurrentResultSet().ColumnCount())
	r.result.CurrentResultSet().Columns(func(m options.Column) {
		cs[i] = m.Name
		i++
	})

	return cs
}

// TODO: Need to store column types to internal rows cache.
//
//nolint:godox
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	r.nextSet.Do(func() {
		r.result.NextResultSet(context.Background())
	})

	var i int
	yqlTypes := make([]string, r.result.CurrentResultSet().ColumnCount())
	r.result.CurrentResultSet().Columns(func(m options.Column) {
		yqlTypes[i] = m.Type.Yql()
		i++
	})

	return yqlTypes[index]
}

func (r *rows) NextResultSet() error {
	r.nextSet.Do(func() {
	})
	return r.result.NextResultSetErr(context.Background())
}

func (r *rows) HasNextResultSet() bool {
	return r.result.HasNextResultSet()
}

func (r *rows) Next(dst []driver.Value) (err error) {
	r.nextSet.Do(func() {
		err = r.result.NextResultSetErr(context.Background())
	})
	if err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	if err = r.result.Err(); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	if !r.result.NextRow() {
		return io.EOF
	}
	values := make([]indexed.RequiredOrOptional, len(dst))
	for i := range dst {
		values[i] = &valuer{}
	}
	if err = r.result.Scan(values...); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	for i := range values {
		dst[i] = values[i].(*valuer).Value()
	}
	if err = r.result.Err(); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	return nil
}

func (r *rows) Close() error {
	return r.result.Close()
}

type single struct {
	values []sql.NamedArg
}

func (r *single) Columns() (columns []string) {
	for _, v := range r.values {
		columns = append(columns, v.Name)
	}
	return columns
}

func (r *single) Close() error {
	return nil
}

func (r *single) Next(dst []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	for i := range r.values {
		dst[i] = r.values[i].Value
	}
	r.values = nil
	return nil
}
