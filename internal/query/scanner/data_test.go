package scanner

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestDecodedAndDirectData(t *testing.T) {
	columns := []*Ydb.Column{
		{Name: "id", Type: types.TypeToYDB(types.Int32)},
		{Name: "name", Type: types.TypeToYDB(types.NewOptional(types.Text))},
	}
	values := []value.Value{value.Int32Value(42), value.OptionalValue(value.TextValue("text"))}
	for _, direct := range []bool{false, true} {
		t.Run(map[bool]string{false: "decoded", true: "direct"}[direct], func(t *testing.T) {
			data := NewDecodedData(columns, values)
			if direct {
				data = NewDirectData(columns, &mockDirectRow{values: values})
			}
			var id int32
			var name *string
			require.NoError(t, Indexed(data).Scan(&id, &name))
			require.Equal(t, int32(42), id)
			require.Equal(t, "text", *name)
			require.NoError(t, Named(data).ScanNamed(NamedRef("name", &name), NamedRef("id", &id)))
			row := struct {
				ID   int32   `sql:"id"`
				Name *string `sql:"name"`
			}{}
			require.NoError(t, Struct(data).ScanStruct(&row))
			require.Equal(t, id, row.ID)
			require.Equal(t, name, row.Name)
			require.Equal(t, values, data.Values())
			owned := data.Values()
			owned[0] = value.Int32Value(1)
			require.Equal(t, values, data.Values())
		})
	}
}

func TestDirectDataScanError(t *testing.T) {
	expected := errors.New("direct scan error")
	data := NewDirectData([]*Ydb.Column{{Name: "id"}}, &mockDirectRow{err: expected})
	var id int32
	require.ErrorIs(t, Indexed(data).Scan(&id), expected)
	require.ErrorIs(t, Named(data).ScanNamed(NamedRef("id", &id)), expected)
	require.ErrorIs(t, Struct(data).ScanStruct(&struct {
		ID int32 `sql:"id"`
	}{}), expected)
}

func TestNamedDataRetainsFirstColumnIndex(t *testing.T) {
	columns := []*Ydb.Column{{Name: "id"}, {Name: "other"}, {Name: "id"}}
	values := []value.Value{value.Int32Value(1), value.Int32Value(2), value.Int32Value(3)}
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct=%v", direct), func(t *testing.T) {
			data := NewDecodedData(columns, values)
			if direct {
				data = NewDirectData(columns, &mockDirectRow{values: values})
			}
			require.Nil(t, data.columnIndexes, "positional reads do not need a name index")
			var got int32
			require.NoError(t, Named(data).ScanNamed(NamedRef("id", &got)))
			require.Equal(t, int32(1), got, "duplicate names must select the first column")
			require.Equal(t, map[string]int{"id": 0, "other": 1}, data.columnIndexes,
				"named scans must retain the index for later lookups")
			require.NoError(t, Named(data).ScanNamed(NamedRef("other", &got)))
			require.Equal(t, int32(2), got)
			require.ErrorIs(t, Named(data).ScanNamed(NamedRef("missing", &got)), ErrColumnsNotFoundInRow)
		})
	}
}

type mockDirectRow struct {
	values []value.Value
	err    error
}

func (r *mockDirectRow) ScanColumn(column int, dst any) error {
	if r.err != nil {
		return r.err
	}

	return value.CastTo(r.values[column], dst)
}
func (r *mockDirectRow) ColumnValue(column int) value.Value { return r.values[column] }
