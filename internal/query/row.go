package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Row = (*row)(nil)

type row struct {
	scannerIndexed
	scannerNamed
	scannerStruct
}

func newRow(columns []*Ydb.Column, v *Ydb.Value) (*row, error) {
	data := newScannerData(columns, v.GetItems())

	return &row{
		newScannerIndexed(data),
		newScannerNamed(data),
		newScannerStruct(data),
	}, nil
}
