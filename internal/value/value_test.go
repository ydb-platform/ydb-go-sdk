package value

import (
	"testing"
)

func TestValueToString(t *testing.T) {
	for _, tt := range []struct {
		value V
		exp   string
	}{
		{
			value: VoidValue,
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
			value: NullValue(OptionalType{T: TypeBool}),
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
			value: ListValue(4, func(i int) V {
				return Int32Value(int32(i))
			}),
			exp: "List<Int32>((0)(1)(2)(3))",
		},
		{
			value: TupleValue(4, func(i int) V {
				return Int32Value(int32(i))
			}),
			exp: "Tuple<Int32,Int32,Int32,Int32>((0)(1)(2)(3))",
		},
		{
			value: VariantValue(Int32Value(42), 1, VariantType{T: TupleType{
				Elems: []T{
					TypeString,
					TypeInt32,
				},
			}}),
			exp: "Variant<Tuple<String,Int32>>(1=(42))",
		},
		{
			value: VariantValue(Int32Value(42), 1, VariantType{S: StructType{
				Fields: []StructField{
					{"foo", TypeString},
					{"bar", TypeInt32},
				},
			}}),
			exp: "Variant<Struct<foo:String,bar:Int32>>(bar=(42))",
		},
		{
			value: DictValue(4, func(i int) V {
				switch i {
				// Key items.
				case 0:
					return UTF8Value("foo")
				case 2:
					return UTF8Value("bar")
				}
				// Value items.
				switch i {
				case 1:
					return Int32Value(42)
				case 3:
					return Int32Value(43)
				}
				panic("whoa")
			}),
			exp: "Dict<Utf8,Int32>(((foo)(42))((bar)(43)))",
		},
		{
			value: DictValue(4, func(i int) V {
				switch i {
				// Key items.
				case 0:
					return UTF8Value("foo")
				case 2:
					return UTF8Value("bar")
				}
				// Value items.
				if i%2 != 0 {
					return VoidValue
				}
				panic("whoa")
			}),
			exp: "Dict<Utf8,Void>(((foo)(NULL))((bar)(NULL)))",
		},
		{
			value: ZeroValue(OptionalType{T: TypeBool}),
			exp:   "Optional<Bool>(NULL)",
		},
		{
			value: ZeroValue(ListType{T: TypeBool}),
			exp:   "List<Bool>()",
		},
		{
			value: ZeroValue(TypeUUID),
			exp:   "Uuid([0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0])",
		},
	} {
		t.Run("", func(t *testing.T) {
			if got := tt.value.String(); got != tt.exp {
				t.Errorf("got: %s, want: %s", got, tt.exp)
			}
		})
	}
}
