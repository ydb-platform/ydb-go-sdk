package query

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Row = (*Row)(nil)

type Row struct {
	data  atomic.Pointer[scanner.Data]
	part  *wirePart
	index int
}

type rowWithData struct {
	Row

	data scanner.Data
}

func (r *Row) Values() []value.Value {
	return r.scannerData().Values()
}

func NewRow(columns []*Ydb.Column, v *Ydb.Value) *Row {
	return newRow(scanner.NewData(columns, v.GetItems()))
}

func newDecodedRow(columns []*Ydb.Column, values []value.Value) *Row {
	return newRow(scanner.NewDecodedData(columns, values))
}

func newRow(data *scanner.Data) *Row {
	// The constructors pass fresh Data, before its lazy indexes are used.
	r := &rowWithData{data: *data}
	r.Row.data.Store(&r.data)

	return &r.Row
}

func (r *Row) scannerData() *scanner.Data {
	if data := r.data.Load(); data != nil {
		return data
	}

	data := scanner.NewDirectData(r.part.columns, &indexedWireRow{
		Row: r, cells: rowCells{data: r.part.rowBytes(r.index)},
	})
	if r.data.CompareAndSwap(nil, data) {
		return data
	}

	return r.data.Load()
}

func (r *Row) Scan(dst ...any) error {
	var err error
	if r.part != nil {
		err = scanRowBytes(r, dst)
	} else {
		err = scanner.Indexed(r.data.Load()).Scan(dst...)
	}
	if err != nil {
		return xerrors.WithStackTrace(
			xerrors.WithStackTrace(err),
			xerrors.WithSkipDepth(1),
		)
	}

	return nil
}

func (r *Row) ScanNamed(dst ...scanner.NamedDestination) error {
	err := scanner.Named(r.scannerData()).ScanNamed(dst...)
	if err != nil {
		return xerrors.WithStackTrace(
			xerrors.WithStackTrace(err),
			xerrors.WithSkipDepth(1),
		)
	}

	return nil
}

func (r *Row) ScanStruct(dst any, opts ...scanner.ScanStructOption) error {
	err := scanner.Struct(r.scannerData()).ScanStruct(dst, opts...)
	if err != nil {
		return xerrors.WithStackTrace(
			xerrors.WithStackTrace(err),
			xerrors.WithSkipDepth(1),
		)
	}

	return nil
}

func readRow(ctx context.Context, r *streamResult) (_ query.Row, finalErr error) {
	defer func() {
		_ = r.Close(ctx)
	}()

	rs, err := r.nextResultSet(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	row, err := rs.nextRow(ctx)
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return nil, xerrors.WithStackTrace(ErrNoRows)
		}

		return nil, xerrors.WithStackTrace(err)
	}
	if _, ok := row.(*arrowRow); ok {
		row = newDecodedRow(rs.columns, row.Values())
	}

	_, err = rs.nextRow(ctx)
	if err == nil {
		return nil, xerrors.WithStackTrace(ErrMoreThanOneRow)
	}
	if !xerrors.Is(err, io.EOF) {
		return nil, xerrors.WithStackTrace(err)
	}

	_, err = r.NextResultSet(ctx)
	if err == nil {
		return nil, xerrors.WithStackTrace(ErrMoreThanOneResultSet)
	}
	if !xerrors.Is(err, io.EOF) {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}
