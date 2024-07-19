package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
)

type (
	Result interface {
		closer.Closer

		// NextResultSet returns next result set
		NextResultSet(ctx context.Context) (ResultSet, error)

		// Range is experimental API for range iterators available with Go version 1.22+ and flag `GOEXPERIMENT=rangefunc`.
		Range(ctx context.Context) xiter.Seq2[ResultSet, error]

		Stats() stats.QueryStats

		// Err returns error (if happened) on result
		Err() error
	}
	ResultSet interface {
		Index() int
		Columns() []string
		ColumnTypes() []Type
		NextRow(ctx context.Context) (Row, error)

		// Range is experimental API for range iterators available with Go version 1.22+ and flag `GOEXPERIMENT=rangefunc`.
		Range(ctx context.Context) xiter.Seq2[Row, error]
	}
	Row interface {
		Scan(dst ...interface{}) error
		ScanNamed(dst ...NamedDestination) error
		ScanStruct(dst interface{}, opts ...ScanStructOption) error
	}
	Type             = types.Type
	NamedDestination = scanner.NamedDestination
	ScanStructOption = scanner.ScanStructOption
)

func Named(columnName string, destinationValueReference interface{}) (dst NamedDestination) {
	return scanner.NamedRef(columnName, destinationValueReference)
}

func WithScanStructTagName(name string) ScanStructOption {
	return scanner.WithTagName(name)
}

func WithScanStructAllowMissingColumnsFromSelect() ScanStructOption {
	return scanner.WithAllowMissingColumnsFromSelect()
}

func WithScanStructAllowMissingFieldsInStruct() ScanStructOption {
	return scanner.WithAllowMissingFieldsInStruct()
}
