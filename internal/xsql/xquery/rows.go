package xquery

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
)

var (
	_ driver.Rows                           = &rows{}
	_ common.Rows                           = &rows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &rows{}
	_ driver.RowsColumnTypeNullable         = &rows{}

	ignoreColumnPrefixName = "__discard_column_"
)

type rows struct {
	conn   *Conn
	result result.Result

	firstNextSet sync.Once
	nextSet      result.Set
	nextErr      error

	columnsFetchError   error
	allColumns, columns []string
	columnsType         []types.Type
	discarded           []bool

	scanValues []value.Value // pre-allocated buffer reused across row scans
	scanDst    []any         // pre-allocated scan destinations pointing into scanValues
}

func (r *rows) updateColumns() {
	if r.nextErr == nil {
		r.allColumns = r.nextSet.Columns()
		r.columns = make([]string, 0, len(r.allColumns))
		r.discarded = make([]bool, len(r.allColumns))
		for i, v := range r.allColumns {
			r.discarded[i] = strings.HasPrefix(v, ignoreColumnPrefixName)
			if !r.discarded[i] {
				r.columns = append(r.columns, v)
			}
		}
		r.columnsType = r.nextSet.ColumnTypes()
		r.columnsFetchError = r.nextErr

		n := len(r.allColumns)
		r.scanValues = make([]value.Value, n)
		r.scanDst = make([]any, n)
		for i := range r.scanValues {
			r.scanDst[i] = &r.scanValues[i]
		}
	}
}

func (r *rows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (r *rows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

func (r *rows) loadFirstNextSet() {
	ctx := context.Background()
	res, err := r.result.NextResultSet(ctx)
	r.nextErr = err
	r.nextSet = res
	r.updateColumns()
}

func (r *rows) Columns() []string {
	r.firstNextSet.Do(r.loadFirstNextSet)
	if r.columnsFetchError != nil {
		panic(xerrors.WithStackTrace(r.columnsFetchError))
	}

	return r.columns
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	r.firstNextSet.Do(r.loadFirstNextSet)
	if r.columnsFetchError != nil {
		panic(xerrors.WithStackTrace(r.columnsFetchError))
	}

	return r.columnsType[index].Yql()
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	r.firstNextSet.Do(r.loadFirstNextSet)
	if r.columnsFetchError != nil {
		panic(xerrors.WithStackTrace(r.columnsFetchError))
	}
	_, castResult := r.nextSet.ColumnTypes()[index].(interface{ IsOptional() })

	return castResult, true
}

func (r *rows) NextResultSet() (finalErr error) {
	r.firstNextSet.Do(func() {})

	ctx := context.Background()
	res, err := r.result.NextResultSet(ctx)
	r.nextErr = err
	r.nextSet = res

	if errors.Is(r.nextErr, io.EOF) {
		return io.EOF
	}

	if r.nextErr != nil {
		return xerrors.WithStackTrace(r.nextErr)
	}
	r.updateColumns()

	return nil
}

func (r *rows) HasNextResultSet() bool {
	r.firstNextSet.Do(r.loadFirstNextSet)

	return r.nextErr == nil
}

func (r *rows) Next(dst []driver.Value) error {
	r.firstNextSet.Do(r.loadFirstNextSet)
	ctx := context.Background()

	if r.nextErr != nil {
		if errors.Is(r.nextErr, io.EOF) {
			return io.EOF
		}

		return xerrors.WithStackTrace(r.nextErr)
	}

	nextRow, err := r.nextSet.NextRow(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}

		return xerrors.WithStackTrace(err)
	}

	if err = nextRow.Scan(r.scanDst...); err != nil {
		return xerrors.WithStackTrace(err)
	}

	dstI := 0
	for i := range r.scanDst {
		if !r.discarded[i] {
			dst[dstI], err = value.Any(r.scanValues[i])
			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			dst[dstI] = common.ToDatabaseSQLValue(dst[dstI])
			dstI++
		}
	}

	return nil
}

func (r *rows) Close() error {
	ctx := context.Background()

	if r.conn != nil && r.conn.ctx != nil {
		ctx = xcontext.ValueOnly(r.conn.ctx)
	}

	return r.result.Close(ctx)
}
