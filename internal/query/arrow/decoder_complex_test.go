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
	tagged := types.NewTagged(types.Int32, "tag")
	variantStruct := types.NewVariantStruct(
		types.StructField{Name: "number", T: types.Int32}, types.StructField{Name: "text", T: types.Text},
	)
	for _, test := range []struct {
		name  string
		array Array
		typ   types.Type
		want  []value.Value
	}{
		{
			"tagged", &scalarTestArray[int32]{values: []int32{42}}, tagged,
			[]value.Value{value.TaggedValue(tagged, value.Int32Value(42))},
		},
		{
			"optional tagged", ints, types.NewOptional(tagged),
			[]value.Value{value.OptionalValue(value.TaggedValue(tagged, value.Int32Value(42))), value.NullValue(tagged)},
		},
		{
			"empty dict", &structTestArray{present, nil}, types.NewEmptyDict(),
			[]value.Value{value.DictValue(), value.DictValue()},
		},
		{
			"struct", &structTestArray{present, []Array{ints}},
			types.NewStruct(types.StructField{Name: "number", T: types.NewOptional(types.Int32)}),
			[]value.Value{
				value.StructValue(value.StructValueField{Name: "number", V: value.OptionalValue(value.Int32Value(42))}),
				value.StructValue(value.StructValueField{Name: "number", V: value.NullValue(types.Int32)}),
			},
		},
		{
			"list value offsets", &listValueOffsetsTestArray{listTestArray{present, ints, []int32{0, 1, 2}}},
			types.NewList(types.NewOptional(types.Int32)),
			[]value.Value{
				value.ListValue(value.OptionalValue(value.Int32Value(42))), value.ListValue(value.NullValue(types.Int32)),
			},
		},
		{
			"variant struct", &unionTestArray{present, []Array{ints, text}, []int{0, 1}}, variantStruct,
			[]value.Value{
				value.VariantValueStruct(value.Int32Value(42), "number", variantStruct),
				value.VariantValueStruct(value.TextValue("text"), "text", variantStruct),
			},
		},
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
		{ints, types.NewNull(), "null array"},
		{ints, types.NewVoid(), "empty Arrow struct"},
		{&structTestArray{shape, []Array{ints}}, types.NewEmptyList(), "empty Arrow struct"},
		{&structTestArray{shape, nil}, types.NewTuple(types.Int32), "incompatible fields"},
		{&structTestArray{shape, []Array{ints}}, types.NewTuple(types.Text), "field 0"},
		{ints, types.NewList(types.Int32), "expected Arrow list"},
		{&listWithoutOffsetsTestArray{shape, ints}, types.NewList(types.Int32), "does not expose offsets"},
		{ints, types.NewDict(types.Int32, types.Text), "expected Arrow list"},
		{&listTestArray{shape, ints, []int32{0, 1}}, types.NewDict(types.Int32, types.Text), "incompatible fields"},
		{ints, types.NewSet(types.Int32), "expected Arrow list"},
		{&listTestArray{shape, ints, []int32{0, 1}}, types.NewSet(types.Int32), "incompatible fields"},
		{ints, variant, "expected Arrow union"},
		{&unionTestArray{shape, nil, []int{0}}, variant, "expected Arrow union"},
		{&unionTestArray{shape, []Array{ints}, []int{0}}, types.NewVariantTuple(types.Text), "does not match YDB Utf8"},
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

func TestVariantColumnUnsupportedType(t *testing.T) {
	ints := &scalarTestArray[int32]{values: []int32{42}}
	data := &unionTestArray{
		scalarTestArray: scalarTestArray[struct{}]{values: make([]struct{}, 1)},
		fields:          []Array{ints},
		ids:             []int{0},
	}
	_, err := variantColumn[Array](data, types.Int32, []types.Type{types.Int32}, func(int) bool { return true })
	require.ErrorContains(t, err, "unsupported YDB variant type")
}

func TestTimezoneColumns(t *testing.T) {
	for _, test := range []struct {
		physical Array
		want     value.Value
	}{
		{&scalarTestArray[uint16]{values: []uint16{1}}, value.TzDateValue("1970-01-02,UTC")},
		{&scalarTestArray[uint32]{values: []uint32{86401}}, value.TzDatetimeValue("1970-01-02T00:00:01,UTC")},
		{&scalarTestArray[uint64]{values: []uint64{86401123456}}, value.TzTimestampValue("1970-01-02T00:00:01.123456,UTC")},
		{&scalarTestArray[int32]{values: []int32{-1}}, value.TzDate32Value("1969-12-31,UTC")},
		{&scalarTestArray[int64]{values: []int64{-1}}, value.TzDatetime64Value("1969-12-31T23:59:59,UTC")},
		{&scalarTestArray[int64]{values: []int64{-1}}, value.TzTimestamp64Value("1969-12-31T23:59:59.999999,UTC")},
	} {
		t.Run(test.want.Type().Yql(), func(t *testing.T) {
			data := &structTestArray{
				scalarTestArray: scalarTestArray[struct{}]{values: make([]struct{}, 1)},
				fields: []Array{
					test.physical, &scalarTestArray[string]{values: []string{"UTC"}},
				},
			}
			column, err := newColumn[Array](data, test.want.Type())
			require.NoError(t, err)
			require.Equal(t, test.want.Yql(), column.value(0).Yql())
			var scanned value.Value
			require.NoError(t, column.scan(0, &scanned))
			require.True(t, proto.Equal(value.ToYDB(test.want), value.ToYDB(scanned)))
		})
	}
}

func TestTimezoneColumnErrors(t *testing.T) {
	shape := scalarTestArray[struct{}]{values: make([]struct{}, 1)}
	ints := &scalarTestArray[int32]{values: []int32{0}}
	zones := &scalarTestArray[string]{values: []string{"UTC"}}
	for _, test := range []struct {
		name    string
		array   Array
		typ     types.Primitive
		message string
	}{
		{"names", &structTestArray{shape, []Array{ints, ints}}, types.TzDate32, "timezone names"},
		{"unknown zone", &structTestArray{shape, []Array{
			ints, &scalarTestArray[string]{values: []string{"Etc/YDBDoesNotExist"}},
		}}, types.TzDate32, "unknown time zone"},
		{"date", &structTestArray{shape, []Array{zones, zones}}, types.TzDate, "does not match"},
		{"date32", &structTestArray{shape, []Array{zones, zones}}, types.TzDate32, "does not match"},
		{"datetime", &structTestArray{shape, []Array{zones, zones}}, types.TzDatetime, "does not match"},
		{"datetime64", &structTestArray{shape, []Array{zones, zones}}, types.TzDatetime64, "does not match"},
		{"timestamp", &structTestArray{shape, []Array{zones, zones}}, types.TzTimestamp, "does not match"},
		{"timestamp64", &structTestArray{shape, []Array{zones, zones}}, types.TzTimestamp64, "does not match"},
	} {
		t.Run(test.name, func(t *testing.T) {
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

type listValueOffsetsTestArray struct {
	listTestArray
}

func (a *listValueOffsetsTestArray) ValueOffsets(row int) (int64, int64) {
	return int64(a.offsets[row]), int64(a.offsets[row+1])
}

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

type listWithoutOffsetsTestArray struct {
	scalarTestArray[struct{}]

	values Array
}

func (a *listWithoutOffsetsTestArray) ListValues() Array { return a.values }
