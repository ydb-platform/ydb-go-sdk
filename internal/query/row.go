package query

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Row = (*Row)(nil)

type Row struct {
	data *scanner.Data

	indexedScanner interface {
		Scan(dst ...interface{}) error
	}
	namedScanner interface {
		ScanNamed(dst ...scanner.NamedDestination) error
	}
	structScanner interface {
		ScanStruct(dst interface{}, opts ...scanner.ScanStructOption) error
	}
}

func (r Row) Values() []value.Value {
	return r.data.Values()
}

func NewRow(columns []*Ydb.Column, v *Ydb.Value) *Row {
	data := scanner.NewData(columns, v.GetItems())

	return &Row{
		data:           data,
		indexedScanner: scanner.Indexed(data),
		namedScanner:   scanner.Named(data),
		structScanner:  scanner.Struct(data),
	}
}

func (r Row) Scan(dst ...interface{}) error {
	err := r.indexedScanner.Scan(dst...)
	if err != nil {
		return xerrors.WithStackTrace(
			xerrors.WithStackTrace(err),
			xerrors.WithSkipDepth(1),
		)
	}

	return nil
}

func (r Row) ScanNamed(dst ...scanner.NamedDestination) error {
	err := r.namedScanner.ScanNamed(dst...)
	if err != nil {
		return xerrors.WithStackTrace(
			xerrors.WithStackTrace(err),
			xerrors.WithSkipDepth(1),
		)
	}

	return nil
}

func (r Row) ScanStruct(dst interface{}, opts ...scanner.ScanStructOption) error {
	err := r.structScanner.ScanStruct(dst, opts...)
	if err != nil {
		return xerrors.WithStackTrace(
			xerrors.WithStackTrace(err),
			xerrors.WithSkipDepth(1),
		)
	}

	return nil
}

func readRow(ctx context.Context, r *streamResult) (_ *Row, finalErr error) {
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
