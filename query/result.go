package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
)

type (
	Result interface {
		closer.Closer

		NextResultSet(ctx context.Context) (ResultSet, error)
		Err() error
	}
	ResultSet interface {
		NextRow(ctx context.Context) (Row, error)
	}
	Row interface {
		Scan(dst ...interface{}) error
		ScanNamed(dst ...scanner.NamedDestination) error
		ScanStruct(dst interface{}, opts ...scanner.ScanStructOption) error
	}
)

func Named(columnName string, destinationValueReference interface{}) (dst scanner.NamedDestination) {
	return scanner.NamedRef(columnName, destinationValueReference)
}

func WithScanStructTagName(name string) scanner.ScanStructOption {
	return scanner.WithTagName(name)
}

func WithScanStructAllowMissingColumnsFromSelect() scanner.ScanStructOption {
	return scanner.WithAllowMissingColumnsFromSelect()
}

func WithScanStructAllowMissingFieldsInStruct() scanner.ScanStructOption {
	return scanner.WithAllowMissingFieldsInStruct()
}
