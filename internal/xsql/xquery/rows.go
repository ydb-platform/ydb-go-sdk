package xquery

import (
	"context"
	"database/sql/driver"
	"io"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
)

var (
	_                      common.Rows = (*rows)(nil)
	ignoreColumnPrefixName             = "__discard_column_"
)

type (
	resultSet struct {
		set          result.Set
		allColumns   []string
		columns      []string
		columnsTypes []types.Type
		discarded    []bool
	}
	rows struct {
		result  result.Result
		next    *resultSet
		lastErr error

		firstNextResultSetCalled bool
	}
)

func newRows(ctx context.Context, result result.Result) (*rows, error) {
	r := &rows{
		result: result,
	}

	if err := r.nextResultSet(ctx); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func (r *rows) nextResultSet(ctx context.Context) (finalErr error) {
	defer func() {
		if finalErr != nil {
			r.lastErr = finalErr
		}
	}()

	rs, err := r.result.NextResultSet(ctx)
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return io.EOF
		}

		return xerrors.WithStackTrace(err)
	}

	allColumns := rs.Columns()

	r.next = &resultSet{
		set:          rs,
		allColumns:   allColumns,
		columns:      make([]string, 0, len(allColumns)),
		columnsTypes: rs.ColumnTypes(),
		discarded:    make([]bool, len(allColumns)),
	}

	for i, v := range allColumns {
		r.next.discarded[i] = strings.HasPrefix(v, ignoreColumnPrefixName)
		if !r.next.discarded[i] {
			r.next.columns = append(r.next.columns, v)
		}
	}

	return nil
}

func (r *rows) Columns(ctx context.Context) []string {
	return r.next.columns
}

func (r *rows) ColumnTypeDatabaseTypeName(ctx context.Context, index int) string {
	return r.next.columnsTypes[index].Yql()
}

func (r *rows) ColumnTypeNullable(ctx context.Context, index int) (nullable, ok bool) {
	_, castResult := r.next.columnsTypes[index].(interface{ IsOptional() })

	return castResult, true
}

func (r *rows) NextResultSet(ctx context.Context) (finalErr error) {
	if !r.firstNextResultSetCalled {
		r.firstNextResultSetCalled = true

		return nil
	}

	if err := r.nextResultSet(ctx); err != nil {
		if xerrors.Is(err, io.EOF) {
			return io.EOF
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (r *rows) Close(ctx context.Context) error {
	defer func() {
		r.lastErr = io.EOF
	}()

	return r.result.Close(ctx)
}

func (r *rows) HasNextResultSet(ctx context.Context) bool {
	return r.lastErr == nil
}

func (r *rows) Next(ctx context.Context, dst []driver.Value) (finalErr error) {
	if !r.firstNextResultSetCalled {
		r.firstNextResultSetCalled = true
	}

	nextRow, err := r.next.set.NextRow(ctx)
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return io.EOF
		}

		return xerrors.WithStackTrace(err)
	}

	values := xslices.Transform(make([]value.Value, len(r.next.allColumns)), func(v value.Value) any { return &v })
	if err = nextRow.Scan(values...); err != nil {
		return xerrors.WithStackTrace(err)
	}

	dstI := 0
	for i := range values {
		if !r.next.discarded[i] {
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
