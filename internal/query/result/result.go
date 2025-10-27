package result

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type (
	Result interface {
		closer.Closer

		// NextResultSet returns next result set
		NextResultSet(ctx context.Context) (Set, error)

		// ResultSets is experimental API for range iterators available
		// with Go version 1.23+
		ResultSets(ctx context.Context) xiter.Seq2[Set, error]
	}
	ConcurrentResult interface {
		closer.Closer

		NextPart(ctx context.Context) (Part, error)
	}
	Set interface {
		Index() int
		Columns() []string
		ColumnTypes() []types.Type
		NextRow(ctx context.Context) (Row, error)

		// Rows is experimental API for range iterators available with Go version 1.23+
		Rows(ctx context.Context) xiter.Seq2[Row, error]
	}
	ClosableResultSet interface {
		Set
		closer.Closer
	}
	Part interface {
		ResultSetIndex() int64
		ColumnNames() []string
		ColumnTypes() []types.Type

		NextRow(ctx context.Context) (Row, error)
	}
	Row interface {
		Values() []value.Value

		Scan(dst ...interface{}) error
		ScanNamed(dst ...scanner.NamedDestination) error
		ScanStruct(dst interface{}, opts ...scanner.ScanStructOption) error
	}
)
