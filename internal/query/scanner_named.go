package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.NamedScanner = scannerNamed{}

type scannerNamed struct {
	data *scannerData
}

func newScannerNamed(data *scannerData) scannerNamed {
	return scannerNamed{
		data: data,
	}
}

func (s scannerNamed) ScanNamed(dst ...query.NamedDestination) error {
	return xerrors.WithStackTrace(ErrNotImplemented)
}
