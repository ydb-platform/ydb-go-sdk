package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Row = (*row)(nil)

type row struct {
	scanner.IndexedScanner
	scanner.NamedScanner
	scanner.StructScanner
}

func newRow(columns []*Ydb.Column, v *Ydb.Value) (*row, error) {
	data := scanner.Data(columns, v.GetItems())

	return &row{
		scanner.Indexed(data),
		scanner.Named(data),
		scanner.Struct(data),
	}, nil
}
