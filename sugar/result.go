package sugar

import (
	"context"
	"errors"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

type result struct {
	r   query.Result
	rs  query.ResultSet
	row query.Row
}

func (r *result) NextResultSet(ctx context.Context) bool {
	var err error
	r.rs, err = r.r.NextResultSet(ctx)
	if err != nil && errors.Is(err, io.EOF) {
		return false
	}

	return err == nil && r.rs != nil
}

func (r *result) NextRow() bool {
	if r.rs == nil {
		return false
	}

	var err error
	r.row, err = r.rs.NextRow(context.Background())
	if err != nil && errors.Is(err, io.EOF) {
		return false
	}

	return r.row != nil
}

func (r *result) Scan(indexedValues ...indexed.RequiredOrOptional) error {
	values := make([]interface{}, 0, len(indexedValues))
	for _, value := range indexedValues {
		values = append(values, value)
	}

	return r.row.Scan(values...)
}

func (r *result) ScanNamed(namedValues ...named.Value) error {
	values := make([]scanner.NamedDestination, 0, len(namedValues))
	for i := range namedValues {
		values = append(values, scanner.NamedRef(namedValues[i].Name, namedValues[i].Value))
	}

	return r.row.ScanNamed(values...)
}

func (r *result) ScanStruct(dst interface{}) error {
	return r.row.ScanStruct(dst)
}

func (r *result) Err() error {
	return nil
}

func (r *result) Close() error {
	return r.r.Close(context.Background())
}

// Result converts query.Result to iterable result for compatibility with table/result.Result usage
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func Result(r query.Result) *result {
	return &result{
		r: r,
	}
}
