package value

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
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

func BenchmarkMemory(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		func() {
			values := [...]V{
				StringValue([]byte("test")),
				BoolValue(true),
				DateValue(1),
				DatetimeValue(1),
				DecimalValue(
					DecimalType{Precision: 22, Scale: 9},
					[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6},
				),
				DoubleValue(1),
				DyNumberValue("123"),
				FloatValue(1),
				Int8Value(1),
				Int16Value(1),
				Int32Value(1),
				Int64Value(1),
				IntervalValue(1),
				JSONValue("{}"),
				JSONDocumentValue("{}"),
				TimestampValue(1),
				TzDateValue("1"),
				TzDatetimeValue("1"),
				TzTimestampValue("1"),
				Uint8Value(1),
				Uint16Value(1),
				Uint32Value(1),
				Uint64Value(1),
				UTF8Value("1"),
				UUIDValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
				VoidValue,
				YSONValue("{}"),
				ListValue(
					3,
					func(i int) V {
						return Int64Value(int64(i + 1))
					},
				),
				OptionalValue(IntervalValue(1)),
				OptionalValue(OptionalValue(IntervalValue(1))),
				StructValue(
					&StructValueProto{
						Fields: []StructField{
							{
								"series_id",
								TypeFromYDB(Uint64Value(1).ToYDB().Type),
							},
							{
								"title",
								TypeFromYDB(UTF8Value("test").ToYDB().Type),
							},
							{
								"air_date",
								TypeFromYDB(DateValue(1).ToYDB().Type),
							},
							{
								"remove_date",
								TypeFromYDB(
									OptionalValue(TzDatetimeValue("1234")).ToYDB().Type,
								),
							},
						},
						Values: []*Ydb.Value{
							Uint64Value(1).ToYDB().Value,
							UTF8Value("test").ToYDB().Value,
							DateValue(1).ToYDB().Value,
							OptionalValue(TzDatetimeValue("1234")).ToYDB().Value,
						},
					},
				),
				DictValue(8, func(i int) V {
					switch i {
					// Key items.
					case 0:
						return UTF8Value("series_id")
					case 2:
						return UTF8Value("title")
					case 4:
						return UTF8Value("air_date")
					case 6:
						return UTF8Value("remove_date")
					}
					// Value items.
					switch i {
					case 1:
						return Uint64Value(1)
					case 3:
						return Uint64Value(2)
					case 5:
						return Uint64Value(3)
					case 7:
						return Uint64Value(4)
					}
					panic("whoa")
				}),
			}
			_ = TupleValue(len(values), func(i int) V {
				return values[i]
			}).ToYDB()
		}()
	}
}

func TestToYDBFromYDB(t *testing.T) {
	vv := []V{
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
		VoidValue,
		FloatValue(1),
		DoubleValue(1),
		StringValue([]byte("test")),
		DecimalValue(DecimalType{Precision: 22, Scale: 9}, [...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
		DyNumberValue("123"),
		JSONValue("{}"),
		JSONDocumentValue("{}"),
		TzDateValue("1"),
		TzDatetimeValue("1"),
		TzTimestampValue("1"),
		UTF8Value("1"),
		UUIDValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
		YSONValue("{}"),
		TupleValue(4, func(i int) V {
			return Int32Value(int32(i))
		}),
		ListValue(4, func(i int) V {
			return Int32Value(int32(i))
		}),
		OptionalValue(IntervalValue(1)),
		OptionalValue(OptionalValue(IntervalValue(1))),
		StructValue(
			&StructValueProto{
				Fields: []StructField{
					{
						"series_id",
						TypeFromYDB(Uint64Value(1).ToYDB().Type),
					},
					{
						"title",
						TypeFromYDB(UTF8Value("test").ToYDB().Type),
					},
					{
						"air_date",
						TypeFromYDB(DateValue(1).ToYDB().Type),
					},
					{
						"remove_date",
						TypeFromYDB(
							OptionalValue(TzDatetimeValue("1234")).ToYDB().Type,
						),
					},
				},
				Values: []*Ydb.Value{
					Uint64Value(1).ToYDB().Value,
					UTF8Value("test").ToYDB().Value,
					DateValue(1).ToYDB().Value,
					OptionalValue(TzDatetimeValue("1234")).ToYDB().Value,
				},
			},
		),
		DictValue(8, func(i int) V {
			switch i {
			// Key items.
			case 0:
				return UTF8Value("series_id")
			case 2:
				return UTF8Value("title")
			case 4:
				return UTF8Value("air_date")
			case 6:
				return UTF8Value("remove_date")
			}
			// Value items.
			switch i {
			case 1:
				return Uint64Value(1)
			case 3:
				return Uint64Value(2)
			case 5:
				return Uint64Value(3)
			case 7:
				return Uint64Value(4)
			}
			panic("whoa")
		}),
		NullValue(OptionalType{T: TypeBool}),
		VariantValue(Int32Value(42), 1, VariantType{T: TupleType{
			Elems: []T{
				TypeString,
				TypeInt32,
			},
		}}),
		VariantValue(Int32Value(42), 1, VariantType{S: StructType{
			Fields: []StructField{
				{"foo", TypeString},
				{"bar", TypeInt32},
			},
		}}),
		ZeroValue(OptionalType{T: TypeBool}),
		ZeroValue(ListType{T: TypeBool}),
		ZeroValue(TypeUUID),
	}
	for _, v := range vv {
		t.Run(v.String(), func(t *testing.T) {
			value := ToYDB(v)
			if !proto.Equal(value, FromYDB(value.Type, value.Value).ToYDB()) {
				t.Errorf("dual conversion failed. got: %v, want: %v", FromYDB(value.Type, value.Value).ToYDB(), value)
			}
		})
	}
}
