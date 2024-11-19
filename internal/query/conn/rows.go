package conn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xslices"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	_ driver.Rows                           = &rows{}
	_ driver.RowsNextResultSet              = &rows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &rows{}
	_ driver.RowsColumnTypeNullable         = &rows{}
	_ driver.Rows                           = &single{}
)

type rows struct {
	conn   *Conn
	result result.Result

	firstNextSet sync.Once
	nextSet      result.Set
	nextErr      error
}

func (r *rows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (r *rows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

func (r *rows) loadFirstNextSet() {
	ctx := context.Background()
	res, err := r.result.NextResultSet(ctx)
	r.nextErr = err
	r.nextSet = res
}

func (r *rows) Columns() []string {
	r.firstNextSet.Do(r.loadFirstNextSet)
	if r.nextErr != nil {
		panic(r.nextErr)
	}

	return r.nextSet.Columns()
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	r.firstNextSet.Do(r.loadFirstNextSet)
	if r.nextErr != nil {
		panic(r.nextErr)
	}

	return r.nextSet.ColumnTypes()[index].String()
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	r.firstNextSet.Do(r.loadFirstNextSet)
	if r.nextErr != nil {
		panic(r.nextErr)
	}
	_, castResult := r.nextSet.ColumnTypes()[index].(interface{ IsOptional() })

	return castResult, castResult
}

func (r *rows) NextResultSet() (finalErr error) {
	r.firstNextSet.Do(r.loadFirstNextSet)

	ctx := context.Background()
	res, err := r.result.NextResultSet(ctx)
	r.nextErr = err
	if err != nil {
		return err
	}

	r.nextSet = res

	return err
}

func (r *rows) HasNextResultSet() bool {
	return r.nextSet != nil
}

func (r *rows) Next(dst []driver.Value) error {
	r.firstNextSet.Do(r.loadFirstNextSet)
	ctx := context.Background()

	nextRow, err := r.nextSet.NextRow(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return err
		}

		return badconn.Map(xerrors.WithStackTrace(err))
	}

	if err = nextRow.Scan(xslices.Transform(dst, func(v driver.Value) any {
		return &v
	})...); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}

	return nil
}

func (r *rows) Close() error {
	ctx := context.Background()

	return r.result.Close(ctx)
}

type single struct {
	values  []sql.NamedArg
	readAll bool
}

func (r *single) Columns() (columns []string) {
	for i := range r.values {
		columns = append(columns, r.values[i].Name)
	}

	return columns
}

func (r *single) Close() error {
	return nil
}

func (r *single) Next(dst []driver.Value) error {
	if r.values == nil || r.readAll {
		return io.EOF
	}
	for i := range r.values {
		dst[i] = r.values[i].Value
	}
	r.readAll = true

	return nil
}
