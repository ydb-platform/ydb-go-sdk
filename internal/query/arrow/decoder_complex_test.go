package arrow

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestComplexColumns(t *testing.T) {
	shape := scalarTestArray[struct{}]{values: make([]struct{}, 2), nulls: []bool{false, true}}
	present := scalarTestArray[struct{}]{values: make([]struct{}, 2)}
	ints := &scalarTestArray[int32]{values: []int32{42, 0}, nulls: []bool{false, true}}
	text := &scalarTestArray[string]{values: []string{"", "text"}, nulls: []bool{true, false}}
	tuple := types.NewTuple(types.Int32)
	variant := types.NewVariantTuple(types.Int32, types.Text)
	nested := types.NewOptional(types.NewOptional(types.Int32))
	for _, test := range []struct {
		name  string
		array Array
		typ   types.Type
		want  []value.Value
	}{
		{
			"nullable tuple", &structTestArray{shape, []Array{ints}}, types.NewOptional(tuple),
			[]value.Value{value.OptionalValue(value.TupleValue(value.Int32Value(42))), value.NullValue(tuple)},
		},
		{
			"nested optional", &structTestArray{present, []Array{ints}}, nested,
			[]value.Value{
				value.OptionalValue(value.OptionalValue(value.Int32Value(42))),
				value.OptionalValue(value.NullValue(types.Int32)),
			},
		},
		{
			"nullable list", &listTestArray{shape, ints, []int32{0, 1, 2}}, types.NewOptional(types.NewList(types.Int32)),
			[]value.Value{
				value.OptionalValue(value.ListValue(value.Int32Value(42))), value.NullValue(types.NewList(types.Int32)),
			},
		},
		{
			"sparse union", &unionTestArray{present, []Array{ints, text}, []int{0, 1}}, variant,
			[]value.Value{
				value.VariantValueTuple(value.Int32Value(42), 0, variant),
				value.VariantValueTuple(value.TextValue("text"), 1, variant),
			},
		},
		{
			"dense union", &denseUnionTestArray{
				unionTestArray{present, []Array{ints, text}, []int{1, 0}}, []int32{1, 0},
			}, variant,
			[]value.Value{
				value.VariantValueTuple(value.TextValue("text"), 1, variant),
				value.VariantValueTuple(value.Int32Value(42), 0, variant),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			column, err := newColumn(test.array, test.typ)
			require.NoError(t, err)
			for row, want := range test.want {
				got := column.value(row)
				require.True(t, proto.Equal(value.ToYDB(got), value.ToYDB(want)))
				require.Equal(t, want.Yql(), got.Yql())
				var scanned value.Value
				require.NoError(t, column.scan(row, &scanned))
				require.True(t, proto.Equal(value.ToYDB(scanned), value.ToYDB(want)))
			}
		})
	}
}

func TestComplexColumnErrors(t *testing.T) {
	shape := scalarTestArray[struct{}]{values: make([]struct{}, 1)}
	ints := &scalarTestArray[int32]{values: []int32{42}}
	variant := types.NewVariantTuple(types.Int32)
	for _, test := range []struct {
		array   Array
		typ     types.Type
		message string
	}{
		{&structTestArray{shape, nil}, types.NewTuple(types.Int32), "incompatible fields"},
		{&structTestArray{shape, nil}, types.NewOptional(types.NewOptional(types.Int32)), "optional wrapper"},
		{&listTestArray{shape, ints, []int32{0}}, types.NewList(types.Int32), "invalid Arrow list offsets"},
		{&listTestArray{shape, ints, []int32{-1, 1}}, types.NewList(types.Int32), "invalid Arrow list offsets"},
		{&listTestArray{shape, ints, []int32{1, 0}}, types.NewList(types.Int32), "invalid Arrow list offsets"},
		{&listTestArray{shape, ints, []int32{0, 2}}, types.NewList(types.Int32), "invalid Arrow list offsets"},
		{&unionTestArray{shape, []Array{ints}, []int{1}}, variant, "invalid Arrow variant"},
		{&denseUnionTestArray{unionTestArray{shape, []Array{ints}, []int{0}}, []int32{1}}, variant, "invalid Arrow variant"},
		{&scalarTestArray[[]byte]{values: [][]byte{make([]byte, 15)}}, types.UUID, "16 Arrow bytes"},
		{&scalarTestArray[[]byte]{values: [][]byte{make([]byte, 17)}}, types.NewDecimal(22, 9), "16 Arrow bytes"},
		{&structTestArray{shape, []Array{ints}}, types.TzDatetime64, "timezone struct"},
	} {
		t.Run(test.typ.Yql()+"/"+test.message, func(t *testing.T) {
			_, err := newColumn(test.array, test.typ)
			require.ErrorContains(t, err, test.message)
		})
	}
}

func TestWideTimezoneCalendar(t *testing.T) {
	for _, year := range []int{0, 1, 10000} {
		instant := time.Date(year, 1, 1, 12, 34, 56, 123456000, time.UTC)
		data := &structTestArray{
			scalarTestArray: scalarTestArray[struct{}]{values: make([]struct{}, 1)},
			fields: []Array{
				&scalarTestArray[int64]{values: []int64{instant.UnixMicro()}},
				&scalarTestArray[string]{values: []string{"UTC"}},
			},
		}
		column, err := newColumn[Array](data, types.TzTimestamp64)
		require.NoError(t, err)
		expectedYear := year
		if expectedYear == 0 {
			expectedYear = -1
		}
		require.Equal(t, value.TzTimestamp64Value(
			fmt.Sprintf("%d-01-01T12:34:56.123456,UTC", expectedYear)).Yql(), column.value(0).Yql())
	}
}

type structTestArray struct {
	scalarTestArray[struct{}]

	fields []Array
}

func (a *structTestArray) NumField() int     { return len(a.fields) }
func (a *structTestArray) Field(i int) Array { return a.fields[i] }

type listTestArray struct {
	scalarTestArray[struct{}]

	values  Array
	offsets []int32
}

func (a *listTestArray) ListValues() Array { return a.values }
func (a *listTestArray) Offsets() []int32  { return a.offsets }

type unionTestArray struct {
	scalarTestArray[struct{}]

	fields []Array
	ids    []int
}

func (a *unionTestArray) NumFields() int    { return len(a.fields) }
func (a *unionTestArray) Field(i int) Array { return a.fields[i] }
func (a *unionTestArray) ChildID(i int) int { return a.ids[i] }

type denseUnionTestArray struct {
	unionTestArray

	offsets []int32
}

func (a *denseUnionTestArray) ValueOffset(i int) int32 { return a.offsets[i] }
