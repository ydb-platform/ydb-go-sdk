package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.StructScanner = scannerStruct{}

type scannerStruct struct {
	data *scannerData
}

func newScannerStruct(data *scannerData) scannerStruct {
	return scannerStruct{
		data: data,
	}
}

func (s scannerStruct) ScanStruct(dst interface{}) error {
	return xerrors.WithStackTrace(ErrNotImplemented)
}
