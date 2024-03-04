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
		IndexedScanner
		NamedScanner
		StructScanner
	}
	IndexedScanner interface {
		Scan(dst ...interface{}) error
	}
	NamedScanner interface {
		ScanNamed(dst ...scanner.NamedDestination) error
	}
	StructScanner interface {
		ScanStruct(dst interface{}, opts ...scanStructOption) error
	}
	scanStructOption = scanner.ScanStructOption
)

func WithTagName(name string) scanStructOption {
	return scanner.WithTagName(name)
}

func WithAllowMissingColumnsFromSelect() scanStructOption {
	return scanner.WithAllowMissingColumnsFromSelect()
}

func WithAllowMissingFieldsInStruct() scanStructOption {
	return scanner.WithAllowMissingFieldsInStruct()
}
