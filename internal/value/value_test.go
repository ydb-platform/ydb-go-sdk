package value

import (
	"testing"

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
		StringValue([]byte("test")),
		DecimalValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9),
		DyNumberValue("123"),
		JSONValue("{}"),
		JSONDocumentValue("{}"),
		TzDateValue("1"),
		TzDatetimeValue("1"),
		TzTimestampValue("1"),
		UTF8Value("1"),
		UUIDValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
		YSONValue("{}"),
		ListValue(
			Int64Value(1),
			Int64Value(2),
			Int64Value(3),
		),
		OptionalValue(IntervalValue(1)),
		OptionalValue(OptionalValue(IntervalValue(1))),
		StructValue(
			StructValueField{"series_id", Uint64Value(1)},
			StructValueField{"title", UTF8Value("test")},
			StructValueField{"air_date", DateValue(1)},
			StructValueField{"remove_date", OptionalValue(TzDatetimeValue("1234"))},
		),
		DictValue(
			DictValueField{UTF8Value("series_id"), Uint64Value(1)},
			DictValueField{UTF8Value("title"), Uint64Value(2)},
			DictValueField{UTF8Value("air_date"), Uint64Value(3)},
			DictValueField{UTF8Value("remove_date"), Uint64Value(4)},
		),
		NullValue(Optional(Optional(Optional(Primitive(TypeBool))))),
		VariantValue(Int32Value(42), 1, Tuple(
			TypeString,
			TypeInt32,
		)),
		VariantValue(Int32Value(42), 1, Struct(
			StructField{
				Name: "foo",
				T:    TypeString,
			},
			StructField{
				Name: "bar",
				T:    TypeInt32,
			},
		)),
		ZeroValue(TypeUTF8),
		ZeroValue(Struct()),
		ZeroValue(Tuple()),
	)
	for i := 0; i < b.N; i++ {
		a := allocator.New()
		_ = ToYDB(v, a)
		a.Free()
	}
}

func TestValueToString(t *testing.T) {
	for _, tt := range []struct {
		value Value
		exp   string
	}{
		{
			value: VoidValue(),
			exp:   "Void(NULL)",
		},
		{
			value: UTF8Value("foo"),
			exp:   "Utf8(foo)",
		},
		{
			value: StringValue([]byte("foo")),
			exp:   "String([102 111 111])",
		},
		{
			value: BoolValue(true),
			exp:   "Bool(true)",
		},
		{
			value: Int8Value(42),
			exp:   "Int8(42)",
		},
		{
			value: Uint8Value(42),
			exp:   "Uint8(42)",
		},
		{
			value: Int16Value(42),
			exp:   "Int16(42)",
		},
		{
			value: Uint16Value(42),
			exp:   "Uint16(42)",
		},
		{
			value: Int32Value(42),
			exp:   "Int32(42)",
		},
		{
			value: Uint32Value(42),
			exp:   "Uint32(42)",
		},
		{
			value: Int64Value(42),
			exp:   "Int64(42)",
		},
		{
			value: Uint64Value(42),
			exp:   "Uint64(42)",
		},
		{
			value: FloatValue(42),
			exp:   "Float(42)",
		},
		{
			value: DoubleValue(42),
			exp:   "Double(42)",
		},
		{
			value: IntervalValue(42),
			exp:   "Interval(42)",
		},
		{
			value: TimestampValue(42),
			exp:   "Timestamp(42)",
		},
		{
			value: NullValue(TypeInt32),
			exp:   "Optional<Int32>(NULL)",
		},
		{
			value: NullValue(Optional(TypeBool)),
			exp:   "Optional<Optional<Bool>>((NULL))",
		},
		{
			value: OptionalValue(OptionalValue(Int32Value(42))),
			exp:   "Optional<Optional<Int32>>((42))",
		},
		{
			value: OptionalValue(OptionalValue(OptionalValue(Int32Value(42)))),
			exp:   "Optional<Optional<Optional<Int32>>>(((42)))",
		},
		{
			value: ListValue(
				Int32Value(0),
				Int32Value(1),
				Int32Value(2),
				Int32Value(3),
			),
			exp: "List<Int32>((0)(1)(2)(3))",
		},
		{
			value: TupleValue(
				Int32Value(0),
				Int32Value(1),
				Int32Value(2),
				Int32Value(3),
			),
			exp: "Tuple<Int32,Int32,Int32,Int32>((0)(1)(2)(3))",
		},
		{
			value: VariantValue(Int32Value(42), 1, Variant(Tuple(
				TypeString,
				TypeInt32,
			))),
			exp: "Variant<Tuple<String,Int32>>(1=(42))",
		},
		{
			value: VariantValue(Int32Value(42), 1, Variant(Struct(
				StructField{
					Name: "foo",
					T:    TypeString,
				},
				StructField{
					Name: "bar",
					T:    TypeInt32,
				},
			))),
			exp: "Variant<Struct<foo:String,bar:Int32>>(bar=(42))",
		},
		{
			value: DictValue(
				DictValueField{UTF8Value("foo"), Int32Value(42)},
				DictValueField{UTF8Value("bar"), Int32Value(43)},
			),
			exp: "Dict<Utf8,Int32>(((foo)(42))((bar)(43)))",
		},
		{
			value: DictValue(
				DictValueField{UTF8Value("foo"), VoidValue()},
				DictValueField{UTF8Value("bar"), VoidValue()},
			),
			exp: "Dict<Utf8,Void>(((foo)(NULL))((bar)(NULL)))",
		},
		{
			value: ZeroValue(Optional(TypeBool)),
			exp:   "Optional<Bool>(NULL)",
		},
		{
			value: ZeroValue(List(TypeBool)),
			exp:   "List<Bool>()",
		},
		{
			value: ZeroValue(TypeUUID),
			exp:   "Uuid([0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0])",
		},
	} {
		t.Run(tt.exp, func(t *testing.T) {
			if got := tt.value.String(); got != tt.exp {
				t.Errorf("string representations not equals:\n\n -  got: %s\n\n - want: %s", got, tt.exp)
			}
		})
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
		StringValue([]byte("test")),
		DecimalValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9),
		DyNumberValue("123"),
		JSONValue("{}"),
		JSONDocumentValue("{}"),
		TzDateValue("1"),
		TzDatetimeValue("1"),
		TzTimestampValue("1"),
		UTF8Value("1"),
		UUIDValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
		YSONValue("{}"),
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
			StructValueField{"title", UTF8Value("test")},
			StructValueField{"air_date", DateValue(1)},
			StructValueField{"remove_date", OptionalValue(TzDatetimeValue("1234"))},
		),
		DictValue(
			DictValueField{UTF8Value("series_id"), Uint64Value(1)},
			DictValueField{UTF8Value("title"), Uint64Value(2)},
			DictValueField{UTF8Value("air_date"), Uint64Value(3)},
			DictValueField{UTF8Value("remove_date"), Uint64Value(4)},
		),
		NullValue(Primitive(TypeBool)),
		NullValue(Optional(Primitive(TypeBool))),
		VariantValue(Int32Value(42), 1, Tuple(
			TypeString,
			TypeInt32,
		)),
		VariantValue(Int32Value(42), 1, Struct(
			StructField{
				Name: "foo",
				T:    TypeString,
			},
			StructField{
				Name: "bar",
				T:    TypeInt32,
			},
		)),
		ZeroValue(TypeUTF8),
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
