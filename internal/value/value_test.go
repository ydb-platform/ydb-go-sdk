package value

import (
	"math"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

func BenchmarkMemory(b *testing.B) {
	b.ReportAllocs()
	v := TupleValue(
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
		UUIDValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
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
		NullValue(Optional(Optional(Optional(TypeBool)))),
		VariantValueTuple(Int32Value(42), 1, Tuple(
			TypeBytes,
			TypeInt32,
		)),
		VariantValueStruct(Int32Value(42), "bar", Struct(
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
	for i, v := range []Value{
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
		NullValue(TypeBool),
		NullValue(Optional(TypeBool)),
		VariantValueTuple(Int32Value(42), 1, Tuple(
			TypeBytes,
			TypeInt32,
		)),
		VariantValueStruct(Int32Value(42), "bar", Struct(
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
	} {
		t.Run(strconv.Itoa(i)+"."+v.Yql(), func(t *testing.T) {
			a := allocator.New()
			defer a.Free()
			value := ToYDB(v, a)
			dualConversedValue, err := fromYDB(value.Type, value.Value)
			require.NoError(t, err)
			if !proto.Equal(value, ToYDB(dualConversedValue, a)) {
				t.Errorf("dual conversion failed:\n\n - got:  %v\n\n - want: %v", ToYDB(dualConversedValue, a), value)
			}
		})
	}
}

func TestValueYql(t *testing.T) {
	for i, tt := range []struct {
		value   Value
		literal string
	}{
		{
			value:   VoidValue(),
			literal: `Void()`,
		},
		{
			value:   TextValue("some\"text\"with brackets"),
			literal: `"some\"text\"with brackets"u`,
		},
		{
			value:   TextValue(`some text with slashes \ \\ \\\`),
			literal: `"some text with slashes \\ \\\\ \\\\\\"u`,
		},
		{
			value:   BytesValue([]byte("foo")),
			literal: `"foo"`,
		},
		{
			value:   BytesValue([]byte("\xFE\xFF")),
			literal: `"\xfe\xff"`,
		},
		{
			value:   OptionalValue(BytesValue([]byte{0, 1, 2, 3, 4, 5, 6})),
			literal: `Just("\x00\x01\x02\x03\x04\x05\x06")`,
		},
		{
			value:   BoolValue(true),
			literal: `true`,
		},
		{
			value:   Int8Value(42),
			literal: `42t`,
		},
		{
			value:   Uint8Value(42),
			literal: `42ut`,
		},
		{
			value:   Int16Value(42),
			literal: `42s`,
		},
		{
			value:   Uint16Value(42),
			literal: `42us`,
		},
		{
			value:   Int32Value(42),
			literal: `42`,
		},
		{
			value:   Uint32Value(42),
			literal: `42u`,
		},
		{
			value:   Int64Value(42),
			literal: `42l`,
		},
		{
			value:   Uint64Value(42),
			literal: `42ul`,
		},
		{
			value:   Uint64Value(200000000000),
			literal: `200000000000ul`,
		},
		{
			value:   FloatValue(42.2121236),
			literal: `Float("42.212124")`,
		},
		{
			value:   FloatValue(float32(math.Inf(+1))),
			literal: `Float("+Inf")`,
		},
		{
			value:   FloatValue(float32(math.Inf(-1))),
			literal: `Float("-Inf")`,
		},
		{
			value:   FloatValue(float32(math.NaN())),
			literal: `Float("NaN")`,
		},
		{
			value:   DoubleValue(42.2121236192),
			literal: `Double("42.2121236192")`,
		},
		{
			value:   DoubleValue(math.Inf(+1)),
			literal: `Double("+Inf")`,
		},
		{
			value:   DoubleValue(math.Inf(-1)),
			literal: `Double("-Inf")`,
		},
		{
			value:   DoubleValue(math.NaN()),
			literal: `Double("NaN")`,
		},
		{
			value: DateValue(func() uint32 {
				v, _ := time.Parse("2006-01-02", "2022-06-17")
				return uint32(v.Sub(time.Unix(0, 0)) / time.Hour / 24)
			}()),
			literal: `Date("2022-06-17")`,
		},
		{
			value: DatetimeValue(func() uint32 {
				v, _ := time.Parse("2006-01-02 15:04:05", "2022-06-17 05:19:20")
				return uint32(v.UTC().Sub(time.Unix(0, 0)).Seconds())
			}()),
			literal: `Datetime("2022-06-17T05:19:20Z")`,
		},
		{
			value:   TzDateValue("2022-06-17,Europe/Berlin"),
			literal: `TzDate("2022-06-17,Europe/Berlin")`,
		},
		{
			value:   TzDatetimeValue("2022-06-17T05:19:20,Europe/Berlin"),
			literal: `TzDatetime("2022-06-17T05:19:20,Europe/Berlin")`,
		},
		{
			value:   IntervalValueFromDuration(time.Duration(42) * time.Millisecond),
			literal: `Interval("PT0.042000S")`,
		},
		{
			value: TimestampValueFromTime(func() time.Time {
				tt, err := time.Parse(LayoutTimestamp, "1997-12-14T03:09:42.123456Z")
				require.NoError(t, err)
				return tt.UTC()
			}()),
			literal: `Timestamp("1997-12-14T03:09:42.123456Z")`,
		},
		{
			value:   TzTimestampValue("1997-12-14T03:09:42.123456,Europe/Berlin"),
			literal: `TzTimestamp("1997-12-14T03:09:42.123456,Europe/Berlin")`,
		},
		{
			value:   NullValue(TypeInt32),
			literal: `Nothing(Optional<Int32>)`,
		},
		{
			value:   NullValue(Optional(TypeBool)),
			literal: `Nothing(Optional<Optional<Bool>>)`,
		},
		{
			value:   Int32Value(42),
			literal: `42`,
		},
		{
			value:   OptionalValue(Int32Value(42)),
			literal: `Just(42)`,
		},
		{
			value:   OptionalValue(OptionalValue(Int32Value(42))),
			literal: `Just(Just(42))`,
		},
		{
			value:   OptionalValue(OptionalValue(OptionalValue(Int32Value(42)))),
			literal: `Just(Just(Just(42)))`,
		},
		{
			value: ListValue(
				Int32Value(-1),
				Int32Value(0),
				Int32Value(1),
				Int32Value(2),
				Int32Value(3),
			),
			literal: `[-1,0,1,2,3]`,
		},
		{
			value: ListValue(
				Int64Value(0),
				Int64Value(1),
				Int64Value(2),
				Int64Value(3),
			),
			literal: `[0l,1l,2l,3l]`,
		},
		{
			value: SetValue(
				Int64Value(0),
				Int64Value(1),
				Int64Value(2),
				Int64Value(3),
			),
			literal: `{0l,1l,2l,3l}`,
		},
		{
			value: TupleValue(
				Int32Value(0),
				Int64Value(1),
				FloatValue(2),
				TextValue("3"),
			),
			literal: `(0,1l,Float("2"),"3"u)`,
		},
		{
			value: VariantValueTuple(Int32Value(42), 1, Tuple(
				TypeBytes,
				TypeInt32,
			)),
			literal: `Variant(42,"1",Variant<String,Int32>)`,
		},
		{
			value: VariantValueTuple(TextValue("foo"), 1, Tuple(
				TypeBytes,
				TypeText,
			)),
			literal: `Variant("foo"u,"1",Variant<String,Utf8>)`,
		},
		{
			value: VariantValueTuple(BoolValue(true), 0, Tuple(
				TypeBytes,
				TypeInt32,
			)),
			literal: `Variant(true,"0",Variant<String,Int32>)`,
		},
		{
			value: VariantValueStruct(Int32Value(42), "bar", Struct(
				StructField{"foo", TypeBytes},
				StructField{"bar", TypeInt32},
			)),
			literal: `Variant(42,"bar",Variant<'bar':Int32,'foo':String>)`,
		},
		{
			value: StructValue(
				StructValueField{"series_id", Uint64Value(1)},
				StructValueField{"title", TextValue("test")},
				StructValueField{"air_date", DateValue(1)},
			),
			literal: "<|`air_date`:Date(\"1970-01-02\"),`series_id`:1ul,`title`:\"test\"u|>",
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), Int32Value(42)},
				DictValueField{TextValue("bar"), Int32Value(43)},
			),
			literal: `{"bar"u:43,"foo"u:42}`,
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), VoidValue()},
				DictValueField{TextValue("bar"), VoidValue()},
			),
			literal: `{"bar"u:Void(),"foo"u:Void()}`,
		},
		{
			value:   ZeroValue(TypeBool),
			literal: `false`,
		},
		{
			value:   ZeroValue(Optional(TypeBool)),
			literal: `Nothing(Optional<Bool>)`,
		},
		{
			value:   ZeroValue(Tuple(TypeBool, TypeDouble)),
			literal: `(false,Double("0"))`,
		},
		{
			value: ZeroValue(Struct(
				StructField{"foo", TypeBool},
				StructField{"bar", TypeText},
			)),
			literal: "<|`bar`:\"\"u,`foo`:false|>",
		},
		{
			value:   ZeroValue(TypeUUID),
			literal: `Uuid("00000000-0000-0000-0000-000000000000")`,
		},
		{
			value:   DecimalValueFromBigInt(big.NewInt(-1234567890123456), 22, 9),
			literal: `Decimal("-1234567.890123456",22,9)`,
		},
		{
			value:   DyNumberValue("-1234567890123456"),
			literal: `DyNumber("-1234567890123456")`,
		},
		{
			value:   JSONValue("{\"a\":-1234567890123456}"),
			literal: `Json(@@{"a":-1234567890123456}@@)`,
		},
		{
			value:   JSONDocumentValue("{\"a\":-1234567890123456}"),
			literal: `JsonDocument(@@{"a":-1234567890123456}@@)`,
		},
		{
			value:   YSONValue([]byte("<a=1>[3;%false]")),
			literal: `Yson("<a=1>[3;%false]")`,
		},
	} {
		t.Run(strconv.Itoa(i)+"."+tt.literal, func(t *testing.T) {
			require.Equal(t, tt.literal, tt.value.Yql())
		})
	}
}
