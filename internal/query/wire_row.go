package query

import (
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Row = (*wireRow)(nil)

type wireRow struct {
	data  atomic.Pointer[Row]
	part  *wirePart
	index int
}

func (r *wireRow) scannerRow() *Row {
	if row := r.data.Load(); row != nil {
		return row
	}
	row := newRow(scanner.NewDirectData(r.part.columns, &indexedWireRow{
		wireRow: r, cells: rowCells{data: r.part.rowBytes(r.index)},
	}))
	if r.data.CompareAndSwap(nil, row) {
		return row
	}

	return r.data.Load()
}

func (r *wireRow) Values() []value.Value { return r.scannerRow().Values() }

func (r *wireRow) Scan(dst ...any) error {
	if err := scanRowBytes(r, dst); err != nil {
		return xerrors.WithStackTrace(xerrors.WithStackTrace(err), xerrors.WithSkipDepth(1))
	}

	return nil
}

func (r *wireRow) ScanNamed(dst ...scanner.NamedDestination) error {
	return r.scannerRow().ScanNamed(dst...)
}

func (r *wireRow) ScanStruct(dst any, opts ...scanner.ScanStructOption) error {
	return r.scannerRow().ScanStruct(dst, opts...)
}
