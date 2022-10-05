package value

import (
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

func BenchmarkMemory(b *testing.B) {
	b.ReportAllocs()
	v := TupleValue(
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
		UUIDValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
		YSONValue([]byte("{}")),
		ListValue(
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
		NullValue(Optional(Optional(Optional(Primitive(TypeBool))))),
		VariantValue(Int32Value(42), 1, Tuple(
			TypeBytes,
			TypeInt32,
		)),
		VariantValue(Int32Value(42), 1, Struct(
			StructField{
				Name: "foo",
				T:    TypeBytes,
			},
			StructField{
				Name: "bar",
				T:    TypeInt32,
			},
		)),
		ZeroValue(TypeText),
		ZeroValue(Struct()),
		ZeroValue(Tuple()),
	)
	for i := 0; i < b.N; i++ {
		a := allocator.New()
		_ = ToYDB(v, a)
		a.Free()
	}
}

func TestToYDBFromYDB(t *testing.T) {
	vv := []Value{
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
		UUIDValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
		YSONValue([]byte("{}")),
		TupleValue(
			Int64Value(1),
			Int32Value(2),
			Int16Value(3),
			Int8Value(4),
		),
		ListValue(
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
		NullValue(Primitive(TypeBool)),
		NullValue(Optional(Primitive(TypeBool))),
		VariantValue(Int32Value(42), 1, Tuple(
			TypeBytes,
			TypeInt32,
		)),
		VariantValue(Int32Value(42), 1, Struct(
			StructField{
				Name: "foo",
				T:    TypeBytes,
			},
			StructField{
				Name: "bar",
				T:    TypeInt32,
			},
		)),
		ZeroValue(TypeText),
		ZeroValue(Struct()),
		ZeroValue(Tuple()),
	}
	for _, v := range vv {
		t.Run(v.String(), func(t *testing.T) {
			a := allocator.New()
			defer a.Free()
			value := ToYDB(v, a)
			dualConversedValue, err := fromYDB(value.Type, value.Value)
			if err != nil {
				t.Errorf("dual conversion error: %v", err)
			} else if !proto.Equal(value, ToYDB(dualConversedValue, a)) {
				t.Errorf("dual conversion failed:\n\n - got:  %v\n\n - want: %v", ToYDB(dualConversedValue, a), value)
			}
		})
	}
}

func TestValueToString(t *testing.T) {
	for _, tt := range []struct {
		value  Value
		string string
	}{
		{
			value:  VoidValue(),
			string: "",
		},
		{
			value:  TextValue("foo"),
			string: "\"foo\"",
		},
		{
			value:  BytesValue([]byte("foo")),
			string: "[0x66,0x6F,0x6F]",
		},
		{
			value:  BoolValue(true),
			string: "true",
		},
		{
			value:  Int8Value(42),
			string: "42",
		},
		{
			value:  Uint8Value(42),
			string: "42",
		},
		{
			value:  Int16Value(42),
			string: "42",
		},
		{
			value:  Uint16Value(42),
			string: "42",
		},
		{
			value:  Int32Value(42),
			string: "42",
		},
		{
			value:  Uint32Value(42),
			string: "42",
		},
		{
			value:  Int64Value(42),
			string: "42",
		},
		{
			value:  Uint64Value(42),
			string: "42",
		},
		{
			value:  FloatValue(42),
			string: "42",
		},
		{
			value:  DoubleValue(42),
			string: "42",
		},
		{
			value: DateValue(func() uint32 {
				v, _ := time.Parse("2006-01-02", "2022-06-17")
				return uint32(v.Sub(time.Unix(0, 0)) / time.Hour / 24)
			}()),
			string: "\"2022-06-17\"",
		},
		{
			value:  IntervalValueFromDuration(time.Duration(42) * time.Millisecond),
			string: "\"42ms\"",
		},
		{
			value: TimestampValueFromTime(func() time.Time {
				tt, err := time.Parse(LayoutTimestamp, "1997-12-14 03:09:42.123456")
				require.NoError(t, err)
				return tt
			}()),
			string: "\"1997-12-14 03:09:42.123456\"",
		},
		{
			value:  NullValue(TypeInt32),
			string: "NULL",
		},
		{
			value:  NullValue(Optional(TypeBool)),
			string: "NULL",
		},
		{
			value:  OptionalValue(OptionalValue(Int32Value(42))),
			string: "42",
		},
		{
			value:  OptionalValue(OptionalValue(OptionalValue(Int32Value(42)))),
			string: "42",
		},
		{
			value: ListValue(
				Int32Value(0),
				Int32Value(1),
				Int32Value(2),
				Int32Value(3),
			),
			string: "(0,1,2,3)",
		},
		{
			value: TupleValue(
				Int32Value(0),
				Int64Value(1),
				FloatValue(2),
				TextValue("3"),
			),
			string: "(0,1,2,\"3\")",
		},
		{
			value: VariantValue(Int32Value(42), 1, Variant(Tuple(
				TypeBytes,
				TypeInt32,
			))),
			string: "(1:42)",
		},
		{
			value: VariantValue(BoolValue(true), 0, Variant(Tuple(
				TypeBytes,
				TypeInt32,
			))),
			string: "(0:true)",
		},
		{
			value: VariantValue(Int32Value(42), 1, Variant(Struct(
				StructField{"foo", TypeBytes},
				StructField{"bar", TypeInt32},
			))),
			string: "(1:42)",
		},
		{
			value: StructValue(
				StructValueField{"series_id", Uint64Value(1)},
				StructValueField{"title", TextValue("test")},
				StructValueField{"air_date", DateValue(1)},
			),
			string: "{\"series_id\":1,\"title\":\"test\",\"air_date\":\"1970-01-02\"}",
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), Int32Value(42)},
				DictValueField{TextValue("bar"), Int32Value(43)},
			),
			string: "(\"foo\":42,\"bar\":43)",
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), VoidValue()},
				DictValueField{TextValue("bar"), VoidValue()},
			),
			string: "(\"foo\":void,\"bar\":void)",
		},
		{
			value:  ZeroValue(Optional(TypeBool)),
			string: "NULL",
		},
		{
			value:  ZeroValue(List(TypeBool)),
			string: "",
		},
		{
			value:  ZeroValue(Tuple(TypeBool, TypeDouble)),
			string: "",
		},
		{
			value: ZeroValue(Struct(
				StructField{"foo", TypeBool},
				StructField{"bar", TypeText},
			)),
			string: "",
		},
		{
			value:  ZeroValue(Dict(TypeText, TypeTimestamp)),
			string: "",
		},
		{
			value:  ZeroValue(TypeUUID),
			string: "\"00000000000000000000000000000000\"",
		},
		{
			value:  DecimalValueFromBigInt(big.NewInt(-1234567890123456), 22, 9),
			string: "-1234567890123456",
		},
	} {
		t.Run(tt.string, func(t *testing.T) {
			if got := tt.value.String(); got != tt.string {
				t.Errorf("string representations not equals:\n\n -  got: %s\n\n - want: %s", got, tt.string)
			}
		})
	}
}
