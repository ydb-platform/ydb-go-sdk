package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type scannerData struct {
	columns []query.Column
	values  []*Ydb.Value
}

func newScannerData(columns []query.Column, values []*Ydb.Value) *scannerData {
	return &scannerData{
		columns: columns,
		values:  values,
	}
}
