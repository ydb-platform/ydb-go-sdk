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
		format map[string]string
	}{
		{
			value:  VoidValue(),
			string: "VOID",
			format: map[string]string{
				"%v":  "VOID",
				"%+v": "Void(VOID)",
				"%T":  "value.voidValue",
				"%s":  "VOID",
			},
		},
		{
			value:  TextValue("foo"),
			string: "\"foo\"",
			format: map[string]string{
				"%v":  "\"foo\"",
				"%+v": "Text(\"foo\")",
				"%T":  "*value.textValue",
				"%s":  "\"foo\"",
			},
		},
		{
			value:  BytesValue([]byte("foo")),
			string: "[0x66,0x6F,0x6F]",
			format: map[string]string{
				"%v":  "[0x66,0x6F,0x6F]",
				"%+v": "Bytes([0x66,0x6F,0x6F])",
				"%T":  "value.bytesValue",
				"%s":  "[0x66,0x6F,0x6F]",
			},
		},
		{
			value:  BoolValue(true),
			string: "true",
			format: map[string]string{
				"%v":  "true",
				"%+v": "Bool(true)",
				"%T":  "value.boolValue",
				"%s":  "true",
			},
		},
		{
			value:  Int8Value(42),
			string: "42",
			format: map[string]string{
				"%v":  "42",
				"%+v": "Int8(42)",
				"%T":  "value.int8Value",
				"%s":  "42",
				"%d":  "42",
			},
		},
		{
			value:  Uint8Value(42),
			string: "42",
			format: map[string]string{
				"%v":  "42",
				"%+v": "Uint8(42)",
				"%T":  "value.uint8Value",
				"%s":  "42",
				"%d":  "42",
			},
		},
		{
			value:  Int16Value(42),
			string: "42",
			format: map[string]string{
				"%v":  "42",
				"%+v": "Int16(42)",
				"%T":  "value.int16Value",
				"%s":  "42",
				"%d":  "42",
			},
		},
		{
			value:  Uint16Value(42),
			string: "42",
			format: map[string]string{
				"%v":  "42",
				"%+v": "Uint16(42)",
				"%T":  "value.uint16Value",
				"%s":  "42",
				"%d":  "42",
			},
		},
		{
			value:  Int32Value(42),
			string: "42",
			format: map[string]string{
				"%v":  "42",
				"%+v": "Int32(42)",
				"%T":  "value.int32Value",
				"%s":  "42",
				"%d":  "42",
			},
		},
		{
			value:  Uint32Value(42),
			string: "42",
			format: map[string]string{
				"%v":  "42",
				"%+v": "Uint32(42)",
				"%T":  "value.uint32Value",
				"%s":  "42",
				"%d":  "42",
			},
		},
		{
			value:  Int64Value(42),
			string: "42",
			format: map[string]string{
				"%v":  "42",
				"%+v": "Int64(42)",
				"%T":  "value.int64Value",
				"%s":  "42",
				"%d":  "42",
			},
		},
		{
			value:  Uint64Value(42),
			string: "42",
			format: map[string]string{
				"%v":  "42",
				"%+v": "Uint64(42)",
				"%T":  "value.uint64Value",
				"%s":  "42",
				"%d":  "42",
			},
		},
		{
			value:  FloatValue(42.2121236),
			string: "42.212124",
			format: map[string]string{
				"%v":    "42.212124",
				"%+v":   "Float(42.212124)",
				"%T":    "*value.floatValue",
				"%s":    "42.212124",
				"%f":    "42.212124",
				"%0.2f": "42.21",
				"%3.5f": "42.21212",
			},
		},
		{
			value:  DoubleValue(42.2121236192),
			string: "42.2121236192",
			format: map[string]string{
				"%v":    "42.2121236192",
				"%+v":   "Double(42.2121236192)",
				"%T":    "*value.doubleValue",
				"%s":    "42.2121236192",
				"%f":    "42.2121236192",
				"%0.2f": "42.21",
				"%3.5f": "42.21212",
			},
		},
		{
			value: DateValue(func() uint32 {
				v, _ := time.Parse("2006-01-02", "2022-06-17")
				return uint32(v.Sub(time.Unix(0, 0)) / time.Hour / 24)
			}()),
			string: "\"2022-06-17\"",
			format: map[string]string{
				"%v":  "\"2022-06-17\"",
				"%+v": "Date(\"2022-06-17\")",
				"%T":  "value.dateValue",
				"%s":  "\"2022-06-17\"",
			},
		},
		{
			value:  IntervalValueFromDuration(time.Duration(42) * time.Millisecond),
			string: "\"42ms\"",
			format: map[string]string{
				"%v":  "\"42ms\"",
				"%+v": "Interval(\"42ms\")",
				"%T":  "value.intervalValue",
				"%s":  "\"42ms\"",
			},
		},
		{
			value: TimestampValueFromTime(func() time.Time {
				tt, err := time.Parse(LayoutTimestamp, "1997-12-14 03:09:42.123456")
				require.NoError(t, err)
				return tt
			}()),
			string: "\"1997-12-14 03:09:42.123456\"",
			format: map[string]string{
				"%v":  "\"1997-12-14 03:09:42.123456\"",
				"%+v": "Timestamp(\"1997-12-14 03:09:42.123456\")",
				"%T":  "value.timestampValue",
				"%s":  "\"1997-12-14 03:09:42.123456\"",
			},
		},
		{
			value:  NullValue(TypeInt32),
			string: "NULL",
			format: map[string]string{
				"%v":  "NULL",
				"%+v": "Optional<Int32>(NULL)",
				"%T":  "*value.nullValue",
				"%s":  "NULL",
			},
		},
		{
			value:  NullValue(Optional(TypeBool)),
			string: "NULL",
			format: map[string]string{
				"%v":  "NULL",
				"%+v": "Optional<Optional<Bool>>(NULL)",
				"%T":  "*value.nullValue",
				"%s":  "NULL",
			},
		},
		{
			value:  OptionalValue(OptionalValue(Int32Value(42))),
			string: "42",
			format: map[string]string{
				"%v":  "42",
				"%+v": "Optional<Optional<Int32>>(42)",
				"%T":  "*value.optionalValue",
				"%s":  "42",
			},
		},
		{
			value:  OptionalValue(OptionalValue(OptionalValue(Int32Value(42)))),
			string: "42",
			format: map[string]string{
				"%v":  "42",
				"%+v": "Optional<Optional<Optional<Int32>>>(42)",
				"%T":  "*value.optionalValue",
				"%s":  "42",
			},
		},
		{
			value: ListValue(
				Int32Value(0),
				Int32Value(1),
				Int32Value(2),
				Int32Value(3),
			),
			string: "[0,1,2,3]",
			format: map[string]string{
				"%v":  "[0,1,2,3]",
				"%+v": "List<Int32>([0,1,2,3])",
				"%T":  "*value.listValue",
				"%s":  "[0,1,2,3]",
			},
		},
		{
			value: TupleValue(
				Int32Value(0),
				Int64Value(1),
				FloatValue(2),
				TextValue("3"),
			),
			string: "(0,1,2,\"3\")",
			format: map[string]string{
				"%v":  "(0,1,2,\"3\")",
				"%+v": "Tuple<Int32,Int64,Float,Text>(0,1,2,\"3\")",
				"%T":  "*value.tupleValue",
				"%s":  "(0,1,2,\"3\")",
			},
		},
		{
			value: VariantValue(Int32Value(42), 1, Variant(Tuple(
				TypeBytes,
				TypeInt32,
			))),
			string: "(1:42)",
			format: map[string]string{
				"%v":  "(1:42)",
				"%+v": "Variant<Tuple<Bytes,Int32>>(1:42)",
				"%T":  "*value.variantValue",
				"%s":  "(1:42)",
			},
		},
		{
			value: VariantValue(BoolValue(true), 0, Variant(Tuple(
				TypeBytes,
				TypeInt32,
			))),
			string: "(0:true)",
			format: map[string]string{
				"%v":  "(0:true)",
				"%+v": "Variant<Tuple<Bytes,Int32>>(0:true)",
				"%T":  "*value.variantValue",
				"%s":  "(0:true)",
			},
		},
		{
			value: VariantValue(Int32Value(42), 1, Variant(Struct(
				StructField{"foo", TypeBytes},
				StructField{"bar", TypeInt32},
			))),
			string: "(1:42)",
			format: map[string]string{
				"%v":  "(1:42)",
				"%+v": "Variant<Struct<\"foo\":Bytes,\"bar\":Int32>>(1:42)",
				"%T":  "*value.variantValue",
				"%s":  "(1:42)",
			},
		},
		{
			value: StructValue(
				StructValueField{"series_id", Uint64Value(1)},
				StructValueField{"title", TextValue("test")},
				StructValueField{"air_date", DateValue(1)},
			),
			string: "{\"series_id\":1,\"title\":\"test\",\"air_date\":\"1970-01-02\"}",
			format: map[string]string{
				"%v": "{\"series_id\":1,\"title\":\"test\",\"air_date\":\"1970-01-02\"}",
				//nolint:lll
				"%+v": "Struct<\"series_id\":Uint64,\"title\":Text,\"air_date\":Date>({\"series_id\":1,\"title\":\"test\",\"air_date\":\"1970-01-02\"})",
				"%T":  "*value.structValue",
				"%s":  "{\"series_id\":1,\"title\":\"test\",\"air_date\":\"1970-01-02\"}",
			},
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), Int32Value(42)},
				DictValueField{TextValue("bar"), Int32Value(43)},
			),
			string: "{\"foo\":42,\"bar\":43}",
			format: map[string]string{
				"%v":  "{\"foo\":42,\"bar\":43}",
				"%+v": "Dict<Text,Int32>({\"foo\":42,\"bar\":43})",
				"%T":  "*value.dictValue",
				"%s":  "{\"foo\":42,\"bar\":43}",
			},
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), VoidValue()},
				DictValueField{TextValue("bar"), VoidValue()},
			),
			string: "{\"foo\":VOID,\"bar\":VOID}",
			format: map[string]string{
				"%v":  "{\"foo\":VOID,\"bar\":VOID}",
				"%+v": "Dict<Text,Void>({\"foo\":VOID,\"bar\":VOID})",
				"%T":  "*value.dictValue",
				"%s":  "{\"foo\":VOID,\"bar\":VOID}",
			},
		},
		{
			value:  ZeroValue(Optional(TypeBool)),
			string: "NULL",
			format: map[string]string{
				"%v":  "NULL",
				"%+v": "Optional<Bool>(NULL)",
				"%T":  "*value.nullValue",
				"%s":  "NULL",
			},
		},
		{
			value:  ZeroValue(List(TypeBool)),
			string: "[]",
			format: map[string]string{
				"%v":  "[]",
				"%+v": "List<Bool>([])",
				"%T":  "*value.listValue",
				"%s":  "[]",
			},
		},
		{
			value:  ZeroValue(Tuple(TypeBool, TypeDouble)),
			string: "(false,0)",
			format: map[string]string{
				"%v":  "(false,0)",
				"%+v": "Tuple<Bool,Double>(false,0)",
				"%T":  "*value.tupleValue",
				"%s":  "(false,0)",
			},
		},
		{
			value: ZeroValue(Struct(
				StructField{"foo", TypeBool},
				StructField{"bar", TypeText},
			)),
			string: "{\"foo\":false,\"bar\":\"\"}",
			format: map[string]string{
				"%v":  "{\"foo\":false,\"bar\":\"\"}",
				"%+v": "Struct<\"foo\":Bool,\"bar\":Text>({\"foo\":false,\"bar\":\"\"})",
				"%T":  "*value.structValue",
				"%s":  "{\"foo\":false,\"bar\":\"\"}",
			},
		},
		{
			value:  ZeroValue(Dict(TypeText, TypeTimestamp)),
			string: "{}",
			format: map[string]string{
				"%v":  "{}",
				"%+v": "Dict<Text,Timestamp>({})",
				"%T":  "*value.dictValue",
				"%s":  "{}",
			},
		},
		{
			value:  ZeroValue(TypeUUID),
			string: "\"00000000000000000000000000000000\"",
			format: map[string]string{
				"%v":  "\"00000000000000000000000000000000\"",
				"%+v": "Uuid(\"00000000000000000000000000000000\")",
				"%T":  "*value.uuidValue",
				"%s":  "\"00000000000000000000000000000000\"",
			},
		},
		{
			value:  DecimalValueFromBigInt(big.NewInt(-1234567890123456), 22, 9),
			string: "-1234567890123456",
			format: map[string]string{
				"%v":  "-1234567890123456",
				"%+v": "Decimal(22,9)(-1234567890123456)",
				"%T":  "*value.decimalValue",
				"%s":  "-1234567890123456",
			},
		},
	} {
		t.Run(fmt.Sprintf("%+v", tt.value), func(t *testing.T) {
			t.Run("String()", func(t *testing.T) {
				if got := tt.value.String(); got != tt.string {
					t.Errorf("string representations not equals:\n\n -  got: %s\n\n - want: %s", got, tt.string)
				}
			})
			for f := range tt.format {
				t.Run(fmt.Sprintf("Sprintf(\"%s\")", f), func(t *testing.T) {
					if got := fmt.Sprintf(f, tt.value); got != tt.format[f] {
						t.Errorf("string representations not equals:\n\n -  got: %s\n\n - want: %s", got, tt.format[f])
					}
				})
			}
		})
	}
}
