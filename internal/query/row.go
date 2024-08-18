package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Row = (*row)(nil)

type row struct {
	indexedScanner scanner.IndexedScanner
	namedScanner   scanner.NamedScanner
	structScanner  scanner.StructScanner
}

func NewRow(columns []*Ydb.Column, v *Ydb.Value) *row {
	data := scanner.Data(columns, v.GetItems())

	return &row{
		indexedScanner: scanner.Indexed(data),
		namedScanner:   scanner.Named(data),
		structScanner:  scanner.Struct(data),
	}
}

func (r row) Scan(dst ...interface{}) (err error) {
	return r.indexedScanner.Scan(dst...)
}

func (r row) ScanNamed(dst ...scanner.NamedDestination) (err error) {
	return r.namedScanner.ScanNamed(dst...)
}

func (r row) ScanStruct(dst interface{}, opts ...scanner.ScanStructOption) (err error) {
	return r.structScanner.ScanStruct(dst, opts...)
}
