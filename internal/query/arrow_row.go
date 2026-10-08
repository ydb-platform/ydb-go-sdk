package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type arrowRowData struct {
	columns []*Ydb.Column
	batch   arrow.Batch
	rows    []arrowRow
}

type arrowRow struct {
	data  *arrowRowData
	index int
}

var _ query.Row = (*arrowRow)(nil)

func (r *arrowRow) ScanColumn(column int, dst any) error {
	return r.data.batch.Scan(r.index, column, dst)
}

func (r *arrowRow) ColumnValue(column int) value.Value { return r.data.batch.Value(r.index, column) }

func (r *arrowRow) Values() []value.Value { return scanner.NewDirectData(r.data.columns, r).Values() }

func (r *arrowRow) Scan(dst ...any) error {
	err := scanner.Indexed(scanner.NewDirectData(r.data.columns, r)).Scan(dst...)
	if err != nil {
		return xerrors.WithStackTrace(
			xerrors.WithStackTrace(err),
			xerrors.WithSkipDepth(1),
		)
	}

	return nil
}

func (r *arrowRow) ScanNamed(dst ...scanner.NamedDestination) error {
	err := scanner.Named(scanner.NewDirectData(r.data.columns, r)).ScanNamed(dst...)
	if err != nil {
		return xerrors.WithStackTrace(
			xerrors.WithStackTrace(err),
			xerrors.WithSkipDepth(1),
		)
	}

	return nil
}

func (r *arrowRow) ScanStruct(dst any, opts ...scanner.ScanStructOption) error {
	err := scanner.Struct(scanner.NewDirectData(r.data.columns, r)).ScanStruct(dst, opts...)
	if err != nil {
		return xerrors.WithStackTrace(
			xerrors.WithStackTrace(err),
			xerrors.WithSkipDepth(1),
		)
	}

	return nil
}
