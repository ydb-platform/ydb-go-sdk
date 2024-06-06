package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

type (
	Result interface {
		closer.Closer

		NextResultSet(ctx context.Context) (ResultSet, error)
		Err() error
	}
	ResultSet interface {
		Columns() []string
		ColumnTypes() []Type
		NextRow(ctx context.Context) (Row, error)
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
