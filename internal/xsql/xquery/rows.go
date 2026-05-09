package xquery

import (
	"context"
	"database/sql/driver"
	"io"
	"strings"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
)

var (
	_                      common.Rows = (*rows)(nil)
	ignoreColumnPrefixName             = "__discard_column_"
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
}

func (r *rows) updateColumns() {
	r.columnsFetchError = r.nextErr
	if r.nextErr == nil && r.nextSet != nil {
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
	}
}

func (r *rows) panicIfColumnsFetchFailed() {
	if err := r.columnsFetchError; err != nil && !xerrors.Is(err, io.EOF) {
		panic(xerrors.WithStackTrace(err))
	}
}

func (r *rows) loadFirstNextSet(ctx context.Context) {
	res, err := r.result.NextResultSet(ctx)
	r.nextErr = err
	r.nextSet = res
	r.updateColumns()
}

func (r *rows) Columns(ctx context.Context) []string {
	r.firstNextSet.Do(func() {
		r.loadFirstNextSet(ctx)
	})
	r.panicIfColumnsFetchFailed()

	return r.columns
}

func (r *rows) ColumnTypeDatabaseTypeName(ctx context.Context, index int) string {
	r.firstNextSet.Do(func() {
		r.loadFirstNextSet(ctx)
	})
	r.panicIfColumnsFetchFailed()

	return r.columnsType[index].Yql()
}

func (r *rows) ColumnTypeNullable(ctx context.Context, index int) (nullable, ok bool) {
	r.firstNextSet.Do(func() {
		r.loadFirstNextSet(ctx)
	})
	r.panicIfColumnsFetchFailed()
	_, castResult := r.nextSet.ColumnTypes()[index].(interface{ IsOptional() })

	return castResult, true
}

func (r *rows) NextResultSet(ctx context.Context) (finalErr error) {
	r.firstNextSet.Do(func() {})

	res, err := r.result.NextResultSet(ctx)
	r.nextErr = err
	r.nextSet = res
	r.updateColumns()

	if xerrors.Is(r.nextErr, io.EOF) {
		return io.EOF
	}

	if r.nextErr != nil {
		return xerrors.WithStackTrace(r.nextErr)
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

func (r *rows) HasNextResultSet(ctx context.Context) bool {
	r.firstNextSet.Do(func() {
		r.loadFirstNextSet(ctx)
	})

	return r.nextErr == nil
}

func (r *rows) Next(ctx context.Context, dst []driver.Value) error {
	r.firstNextSet.Do(func() {
		r.loadFirstNextSet(ctx)
	})

	if r.nextErr != nil {
		if xerrors.Is(r.nextErr, io.EOF) {
			return io.EOF
		}

		return xerrors.WithStackTrace(r.nextErr)
	}

	nextRow, err := r.nextSet.NextRow(ctx)
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return io.EOF
		}

		return xerrors.WithStackTrace(err)
	}

	values := xslices.Transform(make([]value.Value, len(r.allColumns)), func(v value.Value) any { return &v })
	if err = nextRow.Scan(values...); err != nil {
		return xerrors.WithStackTrace(err)
	}

	dstI := 0
	for i := range values {
		if !r.discarded[i] {
			if v := values[i]; v != nil {
				dst[dstI], err = value.Any(*(v.(*value.Value))) //nolint:forcetypeassert
				if err != nil {
					return xerrors.WithStackTrace(err)
				}

				dst[dstI] = common.ToDatabaseSQLValue(dst[dstI])
			}
			dstI++
		}
	}

	return nil
}
