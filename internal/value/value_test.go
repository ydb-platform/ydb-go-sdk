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
	}
	for _, v := range vv {
		t.Run(v.String(), func(t *testing.T) {
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

func TestValueToString(t *testing.T) {
	for _, tt := range []struct {
		value  Value
		string string
	}{
		{
			value:  VoidValue(),
			string: `Void()`,
		},
		{
			value:  TextValue("some\"text\"with brackets"),
			string: `Utf8("some\"text\"with brackets")`,
		},
		{
			value:  BytesValue([]byte("foo")),
			string: `String("foo")`,
		},
		{
			value:  OptionalValue(BytesValue([]byte("foo"))),
			string: `Just(String("foo"))`,
		},
		{
			value:  BoolValue(true),
			string: `Bool("true")`,
		},
		{
			value:  Int8Value(42),
			string: `Int8("42")`,
		},
		{
			value:  Uint8Value(42),
			string: `Uint8("42")`,
		},
		{
			value:  Int16Value(42),
			string: `Int16("42")`,
		},
		{
			value:  Uint16Value(42),
			string: `Uint16("42")`,
		},
		{
			value:  Int32Value(42),
			string: `Int32("42")`,
		},
		{
			value:  Uint32Value(42),
			string: `Uint32("42")`,
		},
		{
			value:  Int64Value(42),
			string: `Int64("42")`,
		},
		{
			value:  Uint64Value(42),
			string: `Uint64("42")`,
		},
		{
			value:  FloatValue(42.2121236),
			string: `Float("42.212124")`,
		},
		{
			value:  DoubleValue(42.2121236192),
			string: `Double("42.2121236192")`,
		},
		{
			value: DateValue(func() uint32 {
				v, _ := time.Parse("2006-01-02", "2022-06-17")
				return uint32(v.Sub(time.Unix(0, 0)) / time.Hour / 24)
			}()),
			string: `Date("2022-06-17")`,
		},
		{
			value: DatetimeValue(func() uint32 {
				v, _ := time.ParseInLocation("2006-01-02 15:04:05", "2022-06-17 05:19:20", time.Local)
				return uint32(v.UTC().Sub(time.Unix(0, 0)).Seconds())
			}()),
			string: `Datetime("2022-06-17T05:19:20Z")`,
		},
		{
			value:  TzDateValue("2022-06-17,Europe/Berlin"),
			string: `TzDate("2022-06-17,Europe/Berlin")`,
		},
		{
			value:  TzDatetimeValue("2022-06-17T05:19:20,Europe/Berlin"),
			string: `TzDatetime("2022-06-17T05:19:20,Europe/Berlin")`,
		},
		{
			value:  IntervalValueFromDuration(time.Duration(42) * time.Millisecond),
			string: `Interval("PT0.042000S")`,
		},
		{
			value: TimestampValueFromTime(func() time.Time {
				tt, err := time.ParseInLocation(LayoutTimestamp, "1997-12-14T03:09:42.123456Z", time.Local)
				require.NoError(t, err)
				return tt.Local()
			}()),
			string: `Timestamp("1997-12-14T03:09:42.123456Z")`,
		},
		{
			value:  TzTimestampValue("1997-12-14T03:09:42.123456,Europe/Berlin"),
			string: `TzTimestamp("1997-12-14T03:09:42.123456,Europe/Berlin")`,
		},
		{
			value:  NullValue(TypeInt32),
			string: `Nothing(Int32?)`,
		},
		{
			value:  NullValue(Optional(TypeBool)),
			string: `Nothing(Optional<Bool>?)`,
		},
		{
			value:  Int32Value(42),
			string: `Int32("42")`,
		},
		{
			value:  OptionalValue(Int32Value(42)),
			string: `Just(Int32("42"))`,
		},
		{
			value:  OptionalValue(OptionalValue(Int32Value(42))),
			string: `Just(Just(Int32("42")))`,
		},
		{
			value:  OptionalValue(OptionalValue(OptionalValue(Int32Value(42)))),
			string: `Just(Just(Just(Int32("42"))))`,
		},
		{
			value: ListValue(
				Int32Value(0),
				Int32Value(1),
				Int32Value(2),
				Int32Value(3),
			),
			string: `AsList(Int32("0"),Int32("1"),Int32("2"),Int32("3"))`,
		},
		{
			value: TupleValue(
				Int32Value(0),
				Int64Value(1),
				FloatValue(2),
				TextValue("3"),
			),
			string: `AsTuple(Int32("0"),Int64("1"),Float("2"),Utf8("3"))`,
		},
		{
			value: VariantValueTuple(Int32Value(42), 1, Tuple(
				TypeBytes,
				TypeInt32,
			)),
			string: `Variant(42,"1",Variant<String,Int32>)`,
		},
		{
			value: VariantValueTuple(TextValue("foo"), 1, Tuple(
				TypeBytes,
				TypeText,
			)),
			string: `Variant("foo","1",Variant<String,Utf8>)`,
		},
		{
			value: VariantValueTuple(BoolValue(true), 0, Tuple(
				TypeBytes,
				TypeInt32,
			)),
			string: `Variant(true,"0",Variant<String,Int32>)`,
		},
		{
			value: VariantValueStruct(Int32Value(42), "bar", Struct(
				StructField{"foo", TypeBytes},
				StructField{"bar", TypeInt32},
			)),
			string: `Variant(42,"bar",Variant<'bar':Int32,'foo':String>)`,
		},
		{
			value: StructValue(
				StructValueField{"series_id", Uint64Value(1)},
				StructValueField{"title", TextValue("test")},
				StructValueField{"air_date", DateValue(1)},
			),
			string: `AsStruct(Date("1970-01-02") AS ` + "`" + `air_date` + "`" + `,Uint64("1") AS ` + "`" + `series_id` + "`" + `,Utf8("test") AS ` + "`" + `title` + "`" + `)`,
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), Int32Value(42)},
				DictValueField{TextValue("bar"), Int32Value(43)},
			),
			string: `AsDict(AsTuple(Utf8("bar"),Int32("43")),AsTuple(Utf8("foo"),Int32("42")))`,
		},
		{
			value: DictValue(
				DictValueField{TextValue("foo"), VoidValue()},
				DictValueField{TextValue("bar"), VoidValue()},
			),
			string: `AsDict(AsTuple(Utf8("bar"),Void()),AsTuple(Utf8("foo"),Void()))`,
		},
		{
			value:  ZeroValue(TypeBool),
			string: `Bool("false")`,
		},
		{
			value:  ZeroValue(Optional(TypeBool)),
			string: `Nothing(Bool?)`,
		},
		{
			value:  ZeroValue(Tuple(TypeBool, TypeDouble)),
			string: `AsTuple(Bool("false"),Double("0"))`,
		},
		{
			value: ZeroValue(Struct(
				StructField{"foo", TypeBool},
				StructField{"bar", TypeText},
			)),
			string: `AsStruct(Utf8("") AS ` + "`" + `bar` + "`" + `,Bool("false") AS ` + "`" + `foo` + "`" + `)`,
		},
		{
			value:  ZeroValue(TypeUUID),
			string: `Uuid("00000000-0000-0000-0000-000000000000")`,
		},
		{
			value:  DecimalValueFromBigInt(big.NewInt(-1234567890123456), 22, 9),
			string: `Decimal("-1234567.890123456",22,9)`,
		},
		{
			value:  DyNumberValue("-1234567890123456"),
			string: `DyNumber("-1234567890123456")`,
		},
		{
			value:  JSONValue("{\"a\":-1234567890123456}"),
			string: `Json(@@{"a":-1234567890123456}@@)`,
		},
		{
			value:  JSONDocumentValue("{\"a\":-1234567890123456}"),
			string: `JsonDocument(@@{"a":-1234567890123456}@@)`,
		},
		{
			value:  YSONValue([]byte("<a=1>[3;%false]")),
			string: `Yson("<a=1>[3;%false]")`,
		},
	} {
		t.Run(fmt.Sprintf("%+v", tt.value), func(t *testing.T) {
			require.Equal(t, tt.string, tt.value.String())
		})
	}
}
