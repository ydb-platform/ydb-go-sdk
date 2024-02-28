package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.IndexedScanner = scannerIndexed{}

type scannerIndexed struct {
	data *scannerData
}

func newScannerIndexed(data *scannerData) scannerIndexed {
	return scannerIndexed{
		data: data,
	}
}

func (s scannerIndexed) Scan(dst ...interface{}) error {
	if len(dst) != len(s.data.columns) {
		return xerrors.WithStackTrace(errWrongArgumentsCount)
	}

	return xerrors.WithStackTrace(ErrNotImplemented)
}
