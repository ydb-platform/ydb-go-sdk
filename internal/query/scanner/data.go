package scanner

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type DirectRow interface {
	ScanColumn(column int, dst any) error
	ColumnValue(column int) value.Value
}

type Data struct {
	columns       []*Ydb.Column
	values        []*Ydb.Value
	decodedValues []value.Value
	direct        DirectRow
}

func NewData(columns []*Ydb.Column, values []*Ydb.Value) *Data {
	return &Data{
		columns: columns,
		values:  values,
	}
}

func NewDecodedData(columns []*Ydb.Column, values []value.Value) *Data {
	return &Data{columns: columns, decodedValues: values}
}

func NewDirectData(columns []*Ydb.Column, direct DirectRow) *Data {
	return &Data{columns: columns, direct: direct}
}

func (d Data) columnIndex(name string) (int, error) {
	for i := range d.columns {
		if d.columns[i].GetName() == name {
			return i, nil
		}
	}

	return 0, xerrors.WithStackTrace(fmt.Errorf("'%s': %w", name, ErrColumnsNotFoundInRow))
}

func (d Data) scanByIndex(index int, dst any) error {
	if d.direct != nil {
		return d.direct.ScanColumn(index, dst)
	}

	return value.CastTo(d.seekByIndex(index), dst)
}

func (d Data) seekByIndex(idx int) value.Value {
	if d.direct != nil {
		return d.direct.ColumnValue(idx)
	}
	if d.decodedValues != nil {
		return d.decodedValues[idx]
	}

	return value.FromYDB(d.columns[idx].GetType(), d.values[idx])
}

func (d Data) Values() []value.Value {
	values := make([]value.Value, len(d.columns))

	for idx := range values {
		values[idx] = d.seekByIndex(idx)
	}

	return values
}
