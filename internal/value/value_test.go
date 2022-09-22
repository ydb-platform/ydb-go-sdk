package value

import (
	"fmt"
	"reflect"
	"testing"

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

func TestCastNumbers(t *testing.T) {
	numberValues := []struct {
		value  Value
		signed bool
		len    int
	}{
		{
			value:  Uint64Value(1),
			signed: false,
			len:    8,
		},
		{
			value:  Int64Value(2),
			signed: true,
			len:    8,
		},
		{
			value:  Uint32Value(3),
			signed: false,
			len:    4,
		},
		{
			value:  Int32Value(4),
			signed: true,
			len:    4,
		},
		{
			value:  Uint16Value(5),
			signed: false,
			len:    2,
		},
		{
			value:  Int16Value(6),
			signed: true,
			len:    2,
		},
		{
			value:  Uint8Value(7),
			signed: false,
			len:    1,
		},
		{
			value:  Int8Value(8),
			signed: true,
			len:    1,
		},
	}
	numberDestinations := []struct {
		destination interface{}
		signed      bool
		len         int
	}{
		{
			destination: func(v uint64) *uint64 { return &v }(1),
			signed:      false,
			len:         8,
		},
		{
			destination: func(v int64) *int64 { return &v }(2),
			signed:      true,
			len:         8,
		},
		{
			destination: func(v uint32) *uint32 { return &v }(3),
			signed:      false,
			len:         4,
		},
		{
			destination: func(v int32) *int32 { return &v }(4),
			signed:      true,
			len:         4,
		},
		{
			destination: func(v uint16) *uint16 { return &v }(5),
			signed:      false,
			len:         2,
		},
		{
			destination: func(v int16) *int16 { return &v }(6),
			signed:      true,
			len:         2,
		},
		{
			destination: func(v uint8) *uint8 { return &v }(7),
			signed:      false,
			len:         1,
		},
		{
			destination: func(v int8) *int8 { return &v }(8),
			signed:      true,
			len:         1,
		},
	}
	for _, src := range numberValues {
		t.Run(src.value.String(), func(t *testing.T) {
			for _, dst := range numberDestinations {
				t.Run(reflect.ValueOf(dst.destination).Type().Elem().String(), func(t *testing.T) {
					mustErr := false
					if src.len > dst.len {
						mustErr = true
					} else if src.len == dst.len && src.signed != dst.signed {
						mustErr = true
					}
					err := src.value.CastTo(dst.destination)
					require.Equal(t, mustErr, (err != nil), err)
					t.Run("Optional<"+src.value.Type().String()+">", func(t *testing.T) {
						mustErr := false
						if src.len > dst.len {
							mustErr = true
						} else if src.len == dst.len && src.signed != dst.signed {
							mustErr = true
						}
						err := OptionalValue(src.value).CastTo(dst.destination)
						require.Equal(t, mustErr, (err != nil), err)
					})
				})
			}
		})
	}
}

func TestCastOtherTypes(t *testing.T) {
	for _, tt := range []struct {
		v      Value
		dst    interface{}
		result interface{}
		error  bool
	}{
		{
			v:      StringValue([]byte("test")),
			dst:    func(v []byte) *[]byte { return &v }(make([]byte, 0, 10)),
			result: func(v []byte) *[]byte { return &v }([]byte("test")),
			error:  false,
		},
		{
			v:      UTF8Value("test"),
			dst:    func(v []byte) *[]byte { return &v }(make([]byte, 0, 10)),
			result: func(v []byte) *[]byte { return &v }([]byte("test")),
			error:  false,
		},
		{
			v:      StringValue([]byte("test")),
			dst:    func(v string) *string { return &v }(""),
			result: func(v string) *string { return &v }("test"),
			error:  false,
		},
		{
			v:      DoubleValue(123),
			dst:    func(v float64) *float64 { return &v }(9),
			result: func(v float64) *float64 { return &v }(123),
			error:  false,
		},
		{
			v:      DoubleValue(123),
			dst:    func(v float32) *float32 { return &v }(9),
			result: func(v float32) *float32 { return &v }(9),
			error:  true,
		},
		{
			v:      FloatValue(123),
			dst:    func(v float64) *float64 { return &v }(9),
			result: func(v float64) *float64 { return &v }(123),
			error:  false,
		},
		{
			v:      FloatValue(123),
			dst:    func(v float32) *float32 { return &v }(9),
			result: func(v float32) *float32 { return &v }(123),
			error:  false,
		},
		{
			v:      Uint64Value(123),
			dst:    func(v float32) *float32 { return &v }(9),
			result: func(v float32) *float32 { return &v }(9),
			error:  true,
		},
		{
			v:      Uint64Value(123),
			dst:    func(v float64) *float64 { return &v }(9),
			result: func(v float64) *float64 { return &v }(123),
			error:  false,
		},
		{
			v:      OptionalValue(DoubleValue(123)),
			dst:    func(v float64) *float64 { return &v }(9),
			result: func(v float64) *float64 { return &v }(123),
			error:  false,
		},
	} {
		t.Run(fmt.Sprintf("cast %s to %v", tt.v.Type().String(), reflect.ValueOf(tt.dst).Type().Elem()),
			func(t *testing.T) {
				if err := tt.v.CastTo(tt.dst); (err != nil) != tt.error {
					t.Errorf("CastTo() error = %v, want %v", err, tt.error)
				} else if !reflect.DeepEqual(tt.dst, tt.result) {
					t.Errorf("CastTo() result = %v, want %v", tt.dst, tt.result)
				}
			},
		)
	}
}
