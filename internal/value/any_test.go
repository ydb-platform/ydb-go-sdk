package value

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

func TestAny(t *testing.T) {
	for _, tt := range []struct {
		src Value
		exp any
	}{
		{
			src: boolValue(true),
			exp: true,
		},
		{
			src: uint8Value(123),
			exp: uint8(123),
		},
		{
			src: int8Value(123),
			exp: int8(123),
		},
		{
			src: uint16Value(123),
			exp: uint16(123),
		},
		{
			src: int16Value(123),
			exp: int16(123),
		},
		{
			src: uint32Value(123),
			exp: uint32(123),
		},
		{
			src: int32Value(123),
			exp: int32(123),
		},
		{
			src: uint64Value(123),
			exp: uint64(123),
		},
		{
			src: int64Value(123),
			exp: int64(123),
		},
		{
			src: &floatValue{value: 123},
			exp: float32(123),
		},
		{
			src: &doubleValue{value: 123},
			exp: float64(123),
		},
		{
			src: bytesValue("123"),
			exp: []byte("123"),
		},
		{
			src: textValue("123"),
			exp: "123",
		},
		{
			src: dyNumberValue("-1234567890123456"),
			exp: "-1234567890123456",
		},
		{
			src: ysonValue("<a=1>[3;%false]"),
			exp: []byte("<a=1>[3;%false]"),
		},
		{
			src: jsonValue("{}"),
			exp: []byte("{}"),
		},
		{
			src: jsonDocumentValue("{}"),
			exp: []byte("{}"),
		},
		{
			src: &uuidValue{
				value: uuid.MustParse("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"),
			},
			exp: uuid.MustParse("6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"),
		},
		{
			src: dateValue(123),
			exp: time.Unix(int64(123*time.Hour*24/time.Second), 0).Local(),
		},
		{
			src: datetimeValue(123),
			exp: time.Unix(int64(123), 0).Local(),
		},
		{
			src: timestampValue(123),
			exp: time.Unix(0, int64(123*time.Microsecond)).Local(),
		},
		{
			src: intervalValue(123),
			exp: 123 * time.Microsecond,
		},
		{
			src: tzDateValue("2020-05-29,Europe/Berlin"),
			exp: time.Date(2020, time.May, 29, 0, 0, 0, 0, must(time.LoadLocation("Europe/Berlin"))),
		},
		{
			src: tzDatetimeValue("2020-05-29T11:22:54,Europe/Berlin"),
			exp: time.Date(2020, time.May, 29, 11, 22, 54, 0, must(time.LoadLocation("Europe/Berlin"))),
		},
		{
			src: tzTimestampValue("2020-05-29T11:22:54.123456,Europe/Berlin"),
			exp: time.Date(2020, time.May, 29, 11, 22, 54, 123456000, must(time.LoadLocation("Europe/Berlin"))),
		},
		{
			src: TupleValue(
				VoidValue(),
				BoolValue(true),
				Int8Value(1),
				Int16Value(1),
				Int32Value(1),
				Int64Value(1),
				Uint8Value(1),
				Uint16Value(1),
				Uint32Value(1),
				Uint64Value(1),
				DateValue(1),
				DatetimeValue(1),
				TimestampValue(1),
				IntervalValue(1),
				VoidValue(),
				FloatValue(1),
				DoubleValue(1),
				BytesValue([]byte("test")),
				DecimalValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9),
				DyNumberValue("123"),
				JSONValue("{}"),
				JSONDocumentValue("{}"),
				TzDateValue("1"),
				TzDatetimeValue("1"),
				TzTimestampValue("1"),
				TextValue("1"),
				UUIDWithIssue1501Value([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
				YSONValue([]byte("{}")),
				ListValue(
					Int64Value(1),
					Int64Value(2),
					Int64Value(3),
				),
				SetValue(
					Int64Value(1),
					Int64Value(2),
					Int64Value(3),
				),
				OptionalValue(IntervalValue(1)),
				OptionalValue(OptionalValue(IntervalValue(1))),
				StructValue(
					StructValueField{"series_id", Uint64Value(1)},
					StructValueField{"title", TextValue("test")},
					StructValueField{"air_date", DateValue(1)},
					StructValueField{"remove_date", OptionalValue(TzDatetimeValue("1234"))},
				),
				DictValue(
					DictValueField{TextValue("series_id"), Uint64Value(1)},
					DictValueField{TextValue("title"), Uint64Value(2)},
					DictValueField{TextValue("air_date"), Uint64Value(3)},
					DictValueField{TextValue("remove_date"), Uint64Value(4)},
				),
				NullValue(types.NewOptional(types.NewOptional(types.NewOptional(types.Bool)))),
				VariantValueTuple(Int32Value(42), 1, types.NewTuple(
					types.Bytes,
					types.Int32,
				)),
				VariantValueStruct(Int32Value(42), "bar", types.NewStruct(
					types.StructField{
						Name: "foo",
						T:    types.Bytes,
					},
					types.StructField{
						Name: "bar",
						T:    types.Int32,
					},
				)),
				ZeroValue(types.Text),
				ZeroValue(types.NewStruct()),
				ZeroValue(types.NewTuple()),
			),
			exp: TupleValue(
				VoidValue(),
				BoolValue(true),
				Int8Value(1),
				Int16Value(1),
				Int32Value(1),
				Int64Value(1),
				Uint8Value(1),
				Uint16Value(1),
				Uint32Value(1),
				Uint64Value(1),
				DateValue(1),
				DatetimeValue(1),
				TimestampValue(1),
				IntervalValue(1),
				VoidValue(),
				FloatValue(1),
				DoubleValue(1),
				BytesValue([]byte("test")),
				DecimalValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9),
				DyNumberValue("123"),
				JSONValue("{}"),
				JSONDocumentValue("{}"),
				TzDateValue("1"),
				TzDatetimeValue("1"),
				TzTimestampValue("1"),
				TextValue("1"),
				UUIDWithIssue1501Value([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
				YSONValue([]byte("{}")),
				ListValue(
					Int64Value(1),
					Int64Value(2),
					Int64Value(3),
				),
				SetValue(
					Int64Value(1),
					Int64Value(2),
					Int64Value(3),
				),
				OptionalValue(IntervalValue(1)),
				OptionalValue(OptionalValue(IntervalValue(1))),
				StructValue(
					StructValueField{"series_id", Uint64Value(1)},
					StructValueField{"title", TextValue("test")},
					StructValueField{"air_date", DateValue(1)},
					StructValueField{"remove_date", OptionalValue(TzDatetimeValue("1234"))},
				),
				DictValue(
					DictValueField{TextValue("series_id"), Uint64Value(1)},
					DictValueField{TextValue("title"), Uint64Value(2)},
					DictValueField{TextValue("air_date"), Uint64Value(3)},
					DictValueField{TextValue("remove_date"), Uint64Value(4)},
				),
				NullValue(types.NewOptional(types.NewOptional(types.NewOptional(types.Bool)))),
				VariantValueTuple(Int32Value(42), 1, types.NewTuple(
					types.Bytes,
					types.Int32,
				)),
				VariantValueStruct(Int32Value(42), "bar", types.NewStruct(
					types.StructField{
						Name: "foo",
						T:    types.Bytes,
					},
					types.StructField{
						Name: "bar",
						T:    types.Int32,
					},
				)),
				ZeroValue(types.Text),
				ZeroValue(types.NewStruct()),
				ZeroValue(types.NewTuple()),
			),
		},
	} {
		t.Run(tt.src.Type().Yql(), func(t *testing.T) {
			t.Run("NonOptional", func(t *testing.T) {
				got, err := Any(tt.src)
				require.NoError(t, err)
				require.Equal(t, tt.exp, got)
			})
			t.Run("Optional", func(t *testing.T) {
				got, err := Any(OptionalValue(tt.src))
				require.NoError(t, err)
				require.Equal(t, tt.exp, got)
			})
			t.Run("Null", func(t *testing.T) {
				got, err := Any(NullValue(tt.src.Type()))
				require.NoError(t, err)
				require.Equal(t, nil, got)
			})
		})
	}
}
