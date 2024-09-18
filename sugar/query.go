package sugar

import (
	"context"
	"errors"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// UnmarshallRow returns typed object from query.Row
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func UnmarshallRow[T any](row query.Row) (*T, error) {
	var v T
	if err := row.ScanStruct(&v); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return &v, nil
}

// UnmarshalRows returns typed object iterator from query.Row iterator
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func UnmarshalRows[T any](seq xiter.Seq2[query.Row, error], opts ...scanner.ScanStructOption) xiter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		seq(func(row query.Row, err error) bool {
			// The function caller for every pair row, errpr in seq iterator, then call yield for outer iterator
			// for produce T, errpr pair for output

			if errors.Is(err, io.EOF) {
				return false
			}

			var val T
			if err != nil {
				yield(val, err)

				return false
			}

			err = row.ScanStruct(&val, opts...)
			if err != nil {
				yield(val, err)

				return false
			}

			return yield(val, err)
		})
	}
}

// UnmarshallResultSet returns slice of typed objects from given query.ResultSet
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func UnmarshallResultSet[T any](resultSet query.ResultSet) (values []*T, _ error) {
	for {
		row, err := resultSet.NextRow(context.Background())
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				break
			}

			return nil, xerrors.WithStackTrace(err)
		}
		var v T
		if err := row.ScanStruct(&v); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		values = append(values, &v)
	}

	return values, nil
}
