package value

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pg"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
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
	)
	for i := 0; i < b.N; i++ {
		_ = ToYDB(v)
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
		UUIDWithIssue1501Value([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
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
		NullValue(types.Bool),
		NullValue(types.NewOptional(types.Bool)),
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
		PgValue(pg.OIDInt4, "123"),
	} {
		t.Run(strconv.Itoa(i)+"."+v.Yql(), func(t *testing.T) {
			value := ToYDB(v)
			dualConversedValue, err := fromYDB(value.GetType(), value.GetValue())
			require.NoError(t, err)
			if !proto.Equal(value, ToYDB(dualConversedValue)) {
				t.Errorf("dual conversion failed:\n\n - got:  %v\n\n - want: %v", ToYDB(dualConversedValue), value)
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
			value: Date32Value(func() int32 {
				v, _ := time.Parse("2006-01-02", "2022-06-17")

				return int32(v.Sub(time.Unix(0, 0)) / time.Hour / 24)
			}()),
			literal: `Date32("2022-06-17")`,
		},
		{
			value: DatetimeValue(func() uint32 {
				v, _ := time.Parse("2006-01-02 15:04:05", "2022-06-17 05:19:20")

				return uint32(v.UTC().Sub(time.Unix(0, 0)).Seconds())
			}()),
			literal: `Datetime("2022-06-17T05:19:20Z")`,
		},
		{
			value: Datetime64Value(func() int64 {
				v, _ := time.Parse("2006-01-02 15:04:05", "2022-06-17 05:19:20")

				return int64(v.UTC().Sub(time.Unix(0, 0)).Seconds())
			}()),
			literal: `Datetime64("2022-06-17T05:19:20Z")`,
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
			value: Timestamp64ValueFromTime(func() time.Time {
				tt, err := time.Parse(LayoutTimestamp, "1997-12-14T03:09:42.123456Z")
				require.NoError(t, err)

				return tt.UTC()
			}()),
			literal: `Timestamp64("1997-12-14T03:09:42.123456Z")`,
		},
		{
			value:   TzTimestampValue("1997-12-14T03:09:42.123456,Europe/Berlin"),
			literal: `TzTimestamp("1997-12-14T03:09:42.123456,Europe/Berlin")`,
		},
		{
			value:   NullValue(types.Int32),
			literal: `Nothing(Optional<Int32>)`,
		},
		{
			value:   NullValue(types.NewOptional(types.Bool)),
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
			value: VariantValueTuple(Int32Value(42), 1, types.NewTuple(
				types.Bytes,
				types.Int32,
			)),
			literal: `Variant(42,"1",Variant<String,Int32>)`,
		},
		{
			value: VariantValueTuple(TextValue("foo"), 1, types.NewTuple(
				types.Bytes,
				types.Text,
			)),
			literal: `Variant("foo"u,"1",Variant<String,Utf8>)`,
		},
		{
			value: VariantValueTuple(BoolValue(true), 0, types.NewTuple(
				types.Bytes,
				types.Int32,
			)),
			literal: `Variant(true,"0",Variant<String,Int32>)`,
		},
		{
			value: VariantValueStruct(Int32Value(42), "bar", types.NewStruct(
				types.StructField{
					Name: "foo",
					T:    types.Bytes,
				},
				types.StructField{
					Name: "bar",
					T:    types.Int32,
				},
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
			value:   ZeroValue(types.Bool),
			literal: `false`,
		},
		{
			value:   ZeroValue(types.NewOptional(types.Bool)),
			literal: `Nothing(Optional<Bool>)`,
		},
		{
			value:   ZeroValue(types.NewTuple(types.Bool, types.Double)),
			literal: `(false,Double("0"))`,
		},
		{
			value: ZeroValue(types.NewStruct(
				types.StructField{
					Name: "foo",
					T:    types.Bool,
				},
				types.StructField{
					Name: "bar",
					T:    types.Text,
				},
			)),
			literal: "<|`bar`:\"\"u,`foo`:false|>",
		},
		{
			value:   ZeroValue(types.UUID),
			literal: `Uuid("00000000-0000-0000-0000-000000000000")`,
		},
		{
			value:   ZeroValue(types.DyNumber),
			literal: `DyNumber("0")`,
		},
		{
			value:   DecimalValueFromBigInt(big.NewInt(-1234567890123456), 22, 9),
			literal: `Decimal("-1234567.890123456",22,9)`,
		},
		{
			value:   DecimalValueFromBigInt(big.NewInt(12345678), 22, 9),
			literal: `Decimal(".012345678",22,9)`,
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
		{
			value:   PgValue(pg.OIDUnknown, "123"),
			literal: `PgConst("123", PgType(705))`,
		},
		{
			value: FromProtobuf(&Ydb.TypedValue{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TupleType{
						TupleType: &Ydb.TupleType{
							Elements: []*Ydb.Type{
								{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT32,
									},
								},
								{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT64,
									},
								},
								{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_FLOAT,
									},
								},
								{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UTF8,
									},
								},
							},
						},
					},
				},
				Value: &Ydb.Value{
					Items: []*Ydb.Value{
						{
							Value: &Ydb.Value_Int32Value{
								Int32Value: 0,
							},
						},
						{
							Value: &Ydb.Value_Int64Value{
								Int64Value: 1,
							},
						},
						{
							Value: &Ydb.Value_FloatValue{
								FloatValue: 2,
							},
						},
						{
							Value: &Ydb.Value_TextValue{
								TextValue: "3",
							},
						},
					},
				},
			}),
			literal: `(0,1l,Float("2"),"3"u)`,
		},
	} {
		t.Run(strconv.Itoa(i)+"."+tt.literal, func(t *testing.T) {
			pb := tt.value.toYDB()
			fmt.Println(pb)
			require.Equal(t, tt.literal, tt.value.Yql())
		})
	}
}

func TestOptionalValueCastTo(t *testing.T) {
	for _, tt := range []struct {
		name string
		v    *optionalValue
		dst  **string
		exp  interface{}
		err  error
	}{
		{
			name: xtest.CurrentFileLine(),
			v:    OptionalValue(TextValue("test")),
			dst:  func(v *string) **string { return &v }(func(s string) *string { return &s }("")),
			exp:  func(v *string) **string { return &v }(func(s string) *string { return &s }("test")),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			v:    OptionalValue(TextValue("test")),
			dst:  func(v *string) **string { return &v }(func() *string { return nil }()),
			exp:  func(v *string) **string { return &v }(func(s string) *string { return &s }("test")),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			v:    NullValue(types.Text),
			dst:  func(v *string) **string { return &v }(func(s string) *string { return &s }("")),
			exp:  func(v *string) **string { return &v }(func() *string { return nil }()),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			v:    NullValue(types.Text),
			dst:  func(v *string) **string { return &v }(func() *string { return nil }()),
			exp:  func(v *string) **string { return &v }(func() *string { return nil }()),
			err:  nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.v.castTo(tt.dst)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.exp, tt.dst)
			}
		})
	}
}

func TestNullable(t *testing.T) {
	for _, test := range []struct {
		name string
		t    types.Type
		v    interface{}
		exp  Value
	}{
		{
			name: "bool",
			t:    types.Bool,
			v:    func(v bool) *bool { return &v }(true),
			exp:  OptionalValue(BoolValue(true)),
		},
		{
			name: "nil bool",
			t:    types.Bool,
			v:    func() *bool { return nil }(),
			exp:  NullValue(types.Bool),
		},
		{
			name: "int8",
			t:    types.Int8,
			v:    func(v int8) *int8 { return &v }(123),
			exp:  OptionalValue(Int8Value(123)),
		},
		{
			name: "nil int8",
			t:    types.Int8,
			v:    func() *int8 { return nil }(),
			exp:  NullValue(types.Int8),
		},
		{
			name: "uint8",
			t:    types.Uint8,
			v:    func(v uint8) *uint8 { return &v }(123),
			exp:  OptionalValue(Uint8Value(123)),
		},
		{
			name: "nil uint8",
			t:    types.Uint8,
			v:    func() *uint8 { return nil }(),
			exp:  NullValue(types.Uint8),
		},
		{
			name: "int16",
			t:    types.Int16,
			v:    func(v int16) *int16 { return &v }(123),
			exp:  OptionalValue(Int16Value(123)),
		},
		{
			name: "nil int16",
			t:    types.Int16,
			v:    func() *int16 { return nil }(),
			exp:  NullValue(types.Int16),
		},
		{
			name: "uint16",
			t:    types.Uint16,
			v:    func(v uint16) *uint16 { return &v }(123),
			exp:  OptionalValue(Uint16Value(123)),
		},
		{
			name: "nil uint16",
			t:    types.Uint16,
			v:    func() *uint16 { return nil }(),
			exp:  NullValue(types.Uint16),
		},
		{
			name: "int32",
			t:    types.Int32,
			v:    func(v int32) *int32 { return &v }(123),
			exp:  OptionalValue(Int32Value(123)),
		},
		{
			name: "nil int32",
			t:    types.Int32,
			v:    func() *int32 { return nil }(),
			exp:  NullValue(types.Int32),
		},
		{
			name: "uint32",
			t:    types.Uint32,
			v:    func(v uint32) *uint32 { return &v }(123),
			exp:  OptionalValue(Uint32Value(123)),
		},
		{
			name: "nil uint32",
			t:    types.Uint32,
			v:    func() *uint32 { return nil }(),
			exp:  NullValue(types.Uint32),
		},
		{
			name: "int64",
			t:    types.Int64,
			v:    func(v int64) *int64 { return &v }(123),
			exp:  OptionalValue(Int64Value(123)),
		},
		{
			name: "nil int64",
			t:    types.Int64,
			v:    func() *int64 { return nil }(),
			exp:  NullValue(types.Int64),
		},
		{
			name: "uint64",
			t:    types.Uint64,
			v:    func(v uint64) *uint64 { return &v }(123),
			exp:  OptionalValue(Uint64Value(123)),
		},
		{
			name: "nil uint64",
			t:    types.Uint64,
			v:    func() *uint64 { return nil }(),
			exp:  NullValue(types.Uint64),
		},
		{
			name: "float",
			t:    types.Float,
			v:    func(v float32) *float32 { return &v }(123),
			exp:  OptionalValue(FloatValue(123)),
		},
		{
			name: "nil float",
			t:    types.Float,
			v:    func() *float32 { return nil }(),
			exp:  NullValue(types.Float),
		},
		{
			name: "double",
			t:    types.Double,
			v:    func(v float64) *float64 { return &v }(123),
			exp:  OptionalValue(DoubleValue(123)),
		},
		{
			name: "nil float",
			t:    types.Double,
			v:    func() *float64 { return nil }(),
			exp:  NullValue(types.Double),
		},
		{
			name: "date from int32",
			t:    types.Date,
			v:    func(v uint32) *uint32 { return &v }(123),
			exp:  OptionalValue(DateValue(123)),
		},
		{
			name: "date from time.Time",
			t:    types.Date,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(DateValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil date",
			t:    types.Date,
			v:    func() *uint32 { return nil }(),
			exp:  NullValue(types.Date),
		},
		{
			name: "datetime from int32",
			t:    types.Datetime,
			v:    func(v uint32) *uint32 { return &v }(123),
			exp:  OptionalValue(DatetimeValue(123)),
		},
		{
			name: "datetime from time.Time",
			t:    types.Datetime,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(DatetimeValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil datetime",
			t:    types.Datetime,
			v:    func() *uint32 { return nil }(),
			exp:  NullValue(types.Datetime),
		},
		{
			name: "timestamp from int32",
			t:    types.Timestamp,
			v:    func(v uint64) *uint64 { return &v }(123),
			exp:  OptionalValue(TimestampValue(123)),
		},
		{
			name: "timestamp from time.Time",
			t:    types.Timestamp,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(TimestampValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil timestamp",
			t:    types.Timestamp,
			v:    func() *uint64 { return nil }(),
			exp:  NullValue(types.Timestamp),
		},
		{
			name: "tzDate from int32",
			t:    types.TzDate,
			v:    func(v string) *string { return &v }(""),
			exp:  OptionalValue(TzDateValue("")),
		},
		{
			name: "tzDate from time.Time",
			t:    types.TzDate,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(TzDateValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil tzDate",
			t:    types.TzDate,
			v:    func() *string { return nil }(),
			exp:  NullValue(types.TzDate),
		},
		{
			name: "interval from int64",
			t:    types.Interval,
			v:    func(v int64) *int64 { return &v }(123),
			exp:  OptionalValue(IntervalValue(123)),
		},
		{
			name: "interval from time.Time",
			t:    types.Interval,
			v:    func(v time.Duration) *time.Duration { return &v }(time.Second),
			exp:  OptionalValue(IntervalValueFromDuration(time.Second)),
		},
		{
			name: "nil interval",
			t:    types.Interval,
			v:    func() *int64 { return nil }(),
			exp:  NullValue(types.Interval),
		},
		{
			name: "tzDatetime from int32",
			t:    types.TzDatetime,
			v:    func(v string) *string { return &v }(""),
			exp:  OptionalValue(TzDatetimeValue("")),
		},
		{
			name: "tzTzDatetime from time.Time",
			t:    types.TzDatetime,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(TzDatetimeValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil tzTzDatetime",
			t:    types.TzDatetime,
			v:    func() *string { return nil }(),
			exp:  NullValue(types.TzDatetime),
		},
		{
			name: "tzTimestamp from int32",
			t:    types.TzTimestamp,
			v:    func(v string) *string { return &v }(""),
			exp:  OptionalValue(TzTimestampValue("")),
		},
		{
			name: "TzTimestamp from time.Time",
			t:    types.TzTimestamp,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(TzTimestampValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil TzTimestamp",
			t:    types.TzTimestamp,
			v:    func() *string { return nil }(),
			exp:  NullValue(types.TzTimestamp),
		},
		{
			name: "string",
			t:    types.Bytes,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(BytesValue([]byte("test"))),
		},
		{
			name: "string",
			t:    types.Bytes,
			v:    func(v []byte) *[]byte { return &v }([]byte("test")),
			exp:  OptionalValue(BytesValue([]byte("test"))),
		},
		{
			name: "nil string",
			t:    types.Bytes,
			v:    func() *string { return nil }(),
			exp:  NullValue(types.Bytes),
		},
		{
			name: "utf8",
			t:    types.Text,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(TextValue("test")),
		},
		{
			name: "nil utf8",
			t:    types.Text,
			v:    func() *string { return nil }(),
			exp:  NullValue(types.Text),
		},
		{
			name: "yson",
			t:    types.YSON,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(YSONValue([]byte("test"))),
		},
		{
			name: "yson",
			t:    types.YSON,
			v:    func(v []byte) *[]byte { return &v }([]byte("test")),
			exp:  OptionalValue(YSONValue([]byte("test"))),
		},
		{
			name: "nil yson",
			t:    types.YSON,
			v:    func() *string { return nil }(),
			exp:  NullValue(types.YSON),
		},
		{
			name: "json",
			t:    types.JSON,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(JSONValue("test")),
		},
		{
			name: "json",
			t:    types.JSON,
			v:    func(v []byte) *[]byte { return &v }([]byte("test")),
			exp:  OptionalValue(JSONValue("test")),
		},
		{
			name: "nil json",
			t:    types.JSON,
			v:    func() *string { return nil }(),
			exp:  NullValue(types.JSON),
		},
		{
			name: "uuid",
			t:    types.UUID,
			v:    func(v [16]byte) *[16]byte { return &v }([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			exp:  OptionalValue(UUIDWithIssue1501Value([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
		},
		{
			name: "jsonDocument",
			t:    types.JSONDocument,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(JSONDocumentValue("test")),
		},
		{
			name: "jsonDocument",
			t:    types.JSONDocument,
			v:    func(v []byte) *[]byte { return &v }([]byte("test")),
			exp:  OptionalValue(JSONDocumentValue("test")),
		},
		{
			name: "nil jsonDocument",
			t:    types.JSONDocument,
			v:    func() *string { return nil }(),
			exp:  NullValue(types.JSONDocument),
		},
		{
			name: "dyNumber",
			t:    types.DyNumber,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(DyNumberValue("test")),
		},
		{
			name: "nil dyNumber",
			t:    types.DyNumber,
			v:    func() *string { return nil }(),
			exp:  NullValue(types.DyNumber),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			v := Nullable(test.t, test.v)
			if !proto.Equal(ToYDB(v), ToYDB(test.exp)) {
				t.Fatalf("unexpected value: %v, exp: %v", v, test.exp)
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
			value:  Int64Value(-2),
			signed: true,
			len:    8,
		},
		{
			value:  Uint32Value(3),
			signed: false,
			len:    4,
		},
		{
			value:  Int32Value(-4),
			signed: true,
			len:    4,
		},
		{
			value:  Uint16Value(5),
			signed: false,
			len:    2,
		},
		{
			value:  Int16Value(-6),
			signed: true,
			len:    2,
		},
		{
			value:  Uint8Value(7),
			signed: false,
			len:    1,
		},
		{
			value:  Int8Value(-8),
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
		{
			destination: func(v float32) *float32 { return &v }(7),
			signed:      true,
			len:         4,
		},
		{
			destination: func(v float64) *float64 { return &v }(8),
			signed:      true,
			len:         8,
		},
	}
	for _, dst := range numberDestinations {
		for _, src := range numberValues {
			t.Run(fmt.Sprintf("%s(%s)→%s",
				src.value.Type().Yql(), src.value.Yql(), reflect.ValueOf(dst.destination).Type().Elem().String(),
			), func(t *testing.T) {
				mustErr := false
				switch {
				case src.len == dst.len && src.signed != dst.signed,
					src.len > dst.len,
					src.signed && !dst.signed:
					mustErr = true
				}
				err := CastTo(src.value, dst.destination)
				if mustErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
			t.Run(fmt.Sprintf("Optional(%s(%s))→%s",
				src.value.Type().Yql(), src.value.Yql(), reflect.ValueOf(dst.destination).Type().Elem().String(),
			), func(t *testing.T) {
				mustErr := false
				switch {
				case src.len == dst.len && src.signed != dst.signed,
					src.len > dst.len,
					src.signed && !dst.signed:
					mustErr = true
				}
				err := CastTo(OptionalValue(src.value), dst.destination)
				if mustErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestCastList(t *testing.T) {
	for _, tt := range []struct {
		v      Value
		dst    interface{}
		result interface{}
		error  bool
	}{
		{
			v:      ListValue(Int32Value(12), Int32Value(21), Int32Value(56)),
			dst:    func(v []int64) *[]int64 { return &v }([]int64{}),
			result: func(v []int64) *[]int64 { return &v }([]int64{12, 21, 56}),
			error:  false,
		},
		{
			v:      ListValue(Int32Value(12), Int32Value(21), Int32Value(56)),
			dst:    func(v []int64) *[]int64 { return &v }([]int64{17}),
			result: func(v []int64) *[]int64 { return &v }([]int64{12, 21, 56}),
			error:  false,
		},
		{
			v:      ListValue(BytesValue([]byte("test")), BytesValue([]byte("test2"))),
			dst:    func(v []string) *[]string { return &v }([]string{}),
			result: func(v []string) *[]string { return &v }([]string{"test", "test2"}),
			error:  false,
		},
		{
			v:      ListValue(BytesValue([]byte("test")), BytesValue([]byte("test2"))),
			dst:    func(v []string) *[]string { return &v }([]string{"list"}),
			result: func(v []string) *[]string { return &v }([]string{"test", "test2"}),
			error:  false,
		},
	} {
		t.Run(fmt.Sprintf("%s→%v", tt.v.Type().Yql(), reflect.ValueOf(tt.dst).Type().Elem()),
			func(t *testing.T) {
				if err := CastTo(tt.v, tt.dst); (err != nil) != tt.error {
					t.Errorf("castTo() error = %v, want %v", err, tt.error)
				} else if !reflect.DeepEqual(tt.dst, tt.result) {
					t.Errorf("castTo() result = %+v, want %+v",
						reflect.ValueOf(tt.dst).Elem(),
						reflect.ValueOf(tt.result).Elem(),
					)
				}
			},
		)
	}
}

func TestCastSet(t *testing.T) {
	for _, tt := range []struct {
		v      Value
		dst    interface{}
		result interface{}
		error  bool
	}{
		{
			v:      SetValue(Int32Value(12), Int32Value(21), Int32Value(56)),
			dst:    func(v []int64) *[]int64 { return &v }([]int64{}),
			result: func(v []int64) *[]int64 { return &v }([]int64{12, 21, 56}),
			error:  false,
		},
		{
			v:      SetValue(Int32Value(12), Int32Value(21), Int32Value(56)),
			dst:    func(v []int64) *[]int64 { return &v }([]int64{17}),
			result: func(v []int64) *[]int64 { return &v }([]int64{12, 21, 56}),
			error:  false,
		},
		{
			v:      SetValue(BytesValue([]byte("test")), BytesValue([]byte("test2"))),
			dst:    func(v []string) *[]string { return &v }([]string{}),
			result: func(v []string) *[]string { return &v }([]string{"test", "test2"}),
			error:  false,
		},
		{
			v:      SetValue(BytesValue([]byte("test")), BytesValue([]byte("test2"))),
			dst:    func(v []string) *[]string { return &v }([]string{"list"}),
			result: func(v []string) *[]string { return &v }([]string{"test", "test2"}),
			error:  false,
		},
	} {
		t.Run(fmt.Sprintf("%s→%v", tt.v.Type().Yql(), reflect.ValueOf(tt.dst).Type().Elem()),
			func(t *testing.T) {
				if err := CastTo(tt.v, tt.dst); (err != nil) != tt.error {
					t.Errorf("castTo() error = %v, want %v", err, tt.error)
				} else if !reflect.DeepEqual(tt.dst, tt.result) {
					t.Errorf("castTo() result = %+v, want %+v",
						reflect.ValueOf(tt.dst).Elem(),
						reflect.ValueOf(tt.result).Elem(),
					)
				}
			},
		)
	}
}

func TestCastStruct(t *testing.T) {
	type defaultStruct struct {
		ID  int32  `sql:"id"`
		Str string `sql:"myStr"`
	}
	for _, tt := range []struct {
		v      Value
		dst    interface{}
		result interface{}
		error  bool
	}{
		{
			v: StructValue(
				StructValueField{Name: "id", V: Int32Value(123)},
				StructValueField{Name: "myStr", V: BytesValue([]byte("myStr123"))},
			),
			dst:    func(v defaultStruct) *defaultStruct { return &v }(defaultStruct{1, "myStr1"}),
			result: func(v defaultStruct) *defaultStruct { return &v }(defaultStruct{123, "myStr123"}),
			error:  false,
		},
		{
			v: StructValue(
				StructValueField{Name: "id", V: Int32Value(12)},
				StructValueField{Name: "myStr", V: BytesValue([]byte("myStr12"))},
			),
			dst:    func(v defaultStruct) *defaultStruct { return &v }(defaultStruct{}),
			result: func(v defaultStruct) *defaultStruct { return &v }(defaultStruct{12, "myStr12"}),
			error:  false,
		},
	} {
		t.Run(fmt.Sprintf("%s→%v", tt.v.Type().Yql(), reflect.ValueOf(tt.dst).Type().Elem()),
			func(t *testing.T) {
				if err := CastTo(tt.v, tt.dst); (err != nil) != tt.error {
					t.Errorf("castTo() error = %v, want %v", err, tt.error)
				} else if !reflect.DeepEqual(tt.dst, tt.result) {
					t.Errorf("castTo() result = %+v, want %+v",
						reflect.ValueOf(tt.dst).Elem(),
						reflect.ValueOf(tt.result).Elem(),
					)
				}
			},
		)
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
			v:      BytesValue([]byte("test")),
			dst:    func(v []byte) *[]byte { return &v }(make([]byte, 0, 10)),
			result: func(v []byte) *[]byte { return &v }([]byte("test")),
			error:  false,
		},
		{
			v:      TextValue("test"),
			dst:    func(v []byte) *[]byte { return &v }(make([]byte, 0, 10)),
			result: func(v []byte) *[]byte { return &v }([]byte("test")),
			error:  false,
		},
		{
			v:      BytesValue([]byte("test")),
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
			result: func(v float64) *float64 { return &v }(9),
			error:  true,
		},
		{
			v:      OptionalValue(DoubleValue(123)),
			dst:    func(v float64) *float64 { return &v }(9),
			result: func(v float64) *float64 { return &v }(123),
			error:  false,
		},
	} {
		t.Run(fmt.Sprintf("%s→%v", tt.v.Type().Yql(), reflect.ValueOf(tt.dst).Type().Elem()),
			func(t *testing.T) {
				if err := CastTo(tt.v, tt.dst); (err != nil) != tt.error {
					t.Errorf("castTo() error = %v, want %v", err, tt.error)
				} else if !reflect.DeepEqual(tt.dst, tt.result) {
					t.Errorf("castTo() result = %+v, want %+v",
						reflect.ValueOf(tt.dst).Elem(),
						reflect.ValueOf(tt.result).Elem(),
					)
				}
			},
		)
	}
}
