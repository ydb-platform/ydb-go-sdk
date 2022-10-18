package value

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		NullValue(Optional(Optional(Optional(TypeBool)))),
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
		NullValue(TypeBool),
		NullValue(Optional(TypeBool)),
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
			string: "Void()",
		},
		{
			value:  TextValue("some\"text\"with brackets"),
			string: "Utf8(\"some\\\"text\\\"with brackets\")",
		},
		{
			value:  BytesValue([]byte("foo")),
			string: "String(\"foo\")",
		},
		{
			value:  OptionalValue(BytesValue([]byte("foo"))),
			string: "Optional<String>(\"foo\")",
		},
		{
			value:  BoolValue(true),
			string: "CAST(true AS Bool)",
		},
		{
			value:  Int8Value(42),
			string: "CAST(42 AS Int8)",
		},
		{
			value:  Uint8Value(42),
			string: "CAST(42 AS Uint8)",
		},
		{
			value:  Int16Value(42),
			string: "CAST(42 AS Int16)",
		},
		{
			value:  Uint16Value(42),
			string: "CAST(42 AS Uint16)",
		},
		{
			value:  Int32Value(42),
			string: "CAST(42 AS Int32)",
		},
		{
			value:  Uint32Value(42),
			string: "CAST(42 AS Uint32)",
		},
		{
			value:  Int64Value(42),
			string: "CAST(42 AS Int64)",
		},
		{
			value:  Uint64Value(42),
			string: "CAST(42 AS Uint64)",
		},
		{
			value:  FloatValue(42.2121236),
			string: "CAST(42.2121236 AS Float)",
		},
		{
			value:  DoubleValue(42.2121236192),
			string: "CAST(42.2121236192 AS Double)",
		},
		{
			value: DateValue(func() uint32 {
				v, _ := time.Parse("2006-01-02", "2022-06-17")
				return uint32(v.Sub(time.Unix(0, 0)) / time.Hour / 24)
			}()),
			string: "CAST(\"2022-06-17\" AS Date)",
		},
		{
			value: DatetimeValue(func() uint32 {
				v, _ := time.ParseInLocation("2006-01-02 15:04:05", "2022-06-17 05:19:20", time.Local)
				return uint32(v.Sub(time.Unix(0, 0)).Seconds())
			}()),
			string: "2022-06-17T05:19:20",
		},
		{
			value:  TzDateValue("2022-06-17,Europe/Berlin"),
			string: "2022-06-17,Europe/Berlin",
		},
		{
			value:  TzDatetimeValue("2022-06-17T05:19:20,Europe/Berlin"),
			string: "2022-06-17T05:19:20,Europe/Berlin",
		},
		{
			value:  IntervalValueFromDuration(time.Duration(42) * time.Millisecond),
			string: "42ms",
		},
		{
			value: TimestampValueFromTime(func() time.Time {
				tt, err := time.ParseInLocation(LayoutTimestamp, "1997-12-14T03:09:42.123456Z", time.Local)
				require.NoError(t, err)
				return tt.Local()
			}()),
			string: "1997-12-14T03:09:42.123456",
		},
		{
			value:  TzTimestampValue("1997-12-14T03:09:42.123456,Europe/Berlin"),
			string: "1997-12-14T03:09:42.123456,Europe/Berlin",
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
			string: "[0,1,2,3]",
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
			string: "{1:42}",
		},
		{
			value: VariantValue(TextValue("foo"), 1, Variant(Tuple(
				TypeBytes,
				TypeText,
			))),
			string: "{1:\"foo\"}",
		},
		{
			value: VariantValue(BoolValue(true), 0, Variant(Tuple(
				TypeBytes,
				TypeInt32,
			))),
			string: "{0:true}",
		},
		{
			value: VariantValue(Int32Value(42), 1, Variant(Struct(
				StructField{"foo", TypeBytes},
				StructField{"bar", TypeInt32},
			))),
			string: "{1:42}",
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
			string: "{\"foo\":42,\"bar\":43}",
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), VoidValue()},
				DictValueField{TextValue("bar"), VoidValue()},
			),
			string: "{\"foo\":VOID,\"bar\":VOID}",
		},
		{
			value:  ZeroValue(TypeBool),
			string: "false",
		},
		{
			value:  ZeroValue(Optional(TypeBool)),
			string: "NULL",
		},
		{
			value:  ZeroValue(List(TypeBool)),
			string: "[]",
		},
		{
			value:  ZeroValue(Tuple(TypeBool, TypeDouble)),
			string: "(false,0)",
		},
		{
			value: ZeroValue(Struct(
				StructField{"foo", TypeBool},
				StructField{"bar", TypeText},
			)),
			string: "{\"foo\":false,\"bar\":\"\"}",
		},
		{
			value:  ZeroValue(Dict(TypeText, TypeTimestamp)),
			string: "{}",
		},
		{
			value:  ZeroValue(TypeUUID),
			string: "\"00000000000000000000000000000000\"",
		},
		{
			value:  DecimalValueFromBigInt(big.NewInt(-1234567890123456), 22, 9),
			string: "-1234567890123456",
		},
		{
			value:  DyNumberValue("-1234567890123456"),
			string: "-1234567890123456",
		},
		{
			value:  JSONValue("{\"a\":-1234567890123456}"),
			string: "{\"a\":-1234567890123456}",
		},
		{
			value:  JSONDocumentValue("{\"a\":-1234567890123456}"),
			string: "{\"a\":-1234567890123456}",
		},
		{
			value:  YSONValue([]byte("{\"a\":-1234567890123456}")),
			string: "{\"a\":-1234567890123456}",
		},
	} {
		t.Run(fmt.Sprintf("%+v", tt.value), func(t *testing.T) {
			if got := tt.value.String(); got != tt.string {
				t.Errorf("string representations not equals:\n\n -  got: %s\n\n - want: %s", got, tt.string)
			}
		})
	}
}
