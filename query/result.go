package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
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
	}
	IndexedScanner interface {
		Scan(dst ...interface{}) error
	}
	NamedScanner interface {
		ScanNamed(dst ...NamedDestination) error
	}
	StructScanner interface {
		ScanStruct(dst interface{}) error
	}
	NamedDestination interface {
		Name() string
		Destination() interface{}
	}
)
