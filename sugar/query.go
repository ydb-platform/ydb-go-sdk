package sugar

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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
