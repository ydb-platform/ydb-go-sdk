package scanner

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type data struct {
	columns []*Ydb.Column
	values  []*Ydb.Value
}

func Data(columns []*Ydb.Column, values []*Ydb.Value) *data {
	return &data{
		columns: columns,
		values:  values,
	}
}

func (s data) seekByName(name string) (value.Value, error) {
	for i := range s.columns {
		if s.columns[i].GetName() == name {
			return value.FromYDB(s.columns[i].GetType(), s.values[i]), nil
		}
	}

	return nil, xerrors.WithStackTrace(fmt.Errorf("'%s': %w", name, ErrColumnsNotFoundInRow))
}

func (s data) seekByIndex(idx int) value.Value {
	return value.FromYDB(s.columns[idx].GetType(), s.values[idx])
}
