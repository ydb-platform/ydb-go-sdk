package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type scannerData struct {
	columns []*Ydb.Column
	values  []*Ydb.Value
}

func newScannerData(columns []*Ydb.Column, values []*Ydb.Value) *scannerData {
	return &scannerData{
		columns: columns,
		values:  values,
	}
}
