package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Row = (*Row)(nil)

type Row struct {
	indexedScanner scanner.IndexedScanner
	namedScanner   scanner.NamedScanner
	structScanner  scanner.StructScanner
}

func NewRow(columns []*Ydb.Column, v *Ydb.Value) *Row {
	data := scanner.Data(columns, v.GetItems())

	return &Row{
		indexedScanner: scanner.Indexed(data),
		namedScanner:   scanner.Named(data),
		structScanner:  scanner.Struct(data),
	}
}

func (r Row) Scan(dst ...interface{}) (err error) {
	return r.indexedScanner.Scan(dst...)
}

func (r Row) ScanNamed(dst ...scanner.NamedDestination) (err error) {
	return r.namedScanner.ScanNamed(dst...)
}

func (r Row) ScanStruct(dst interface{}, opts ...scanner.ScanStructOption) (err error) {
	return r.structScanner.ScanStruct(dst, opts...)
}
