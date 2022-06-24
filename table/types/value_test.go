package types

import (
	value "github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func TestNullable(t *testing.T) {
	for _, test := range []struct {
		name string
		t    Type
		v    interface{}
		exp  Value
	}{
		{
			name: "bool",
			t:    TypeBool,
			v:    func(v bool) *bool { return &v }(true),
			exp:  OptionalValue(BoolValue(true)),
		},
		{
			name: "nil bool",
			t:    TypeBool,
			v:    func() *bool { return nil }(),
			exp:  NullValue(TypeBool),
		},
		{
			name: "int8",
			t:    TypeInt8,
			v:    func(v int8) *int8 { return &v }(123),
			exp:  OptionalValue(Int8Value(123)),
		},
		{
			name: "nil int8",
			t:    TypeInt8,
			v:    func() *int8 { return nil }(),
			exp:  NullValue(TypeInt8),
		},
		{
			name: "uint8",
			t:    TypeUint8,
			v:    func(v uint8) *uint8 { return &v }(123),
			exp:  OptionalValue(Uint8Value(123)),
		},
		{
			name: "nil uint8",
			t:    TypeUint8,
			v:    func() *uint8 { return nil }(),
			exp:  NullValue(TypeUint8),
		},
		{
			name: "int16",
			t:    TypeInt16,
			v:    func(v int16) *int16 { return &v }(123),
			exp:  OptionalValue(Int16Value(123)),
		},
		{
			name: "nil int16",
			t:    TypeInt16,
			v:    func() *int16 { return nil }(),
			exp:  NullValue(TypeInt16),
		},
		{
			name: "uint16",
			t:    TypeUint16,
			v:    func(v uint16) *uint16 { return &v }(123),
			exp:  OptionalValue(Uint16Value(123)),
		},
		{
			name: "nil uint16",
			t:    TypeUint16,
			v:    func() *uint16 { return nil }(),
			exp:  NullValue(TypeUint16),
		},
		{
			name: "int32",
			t:    TypeInt32,
			v:    func(v int32) *int32 { return &v }(123),
			exp:  OptionalValue(Int32Value(123)),
		},
		{
			name: "nil int32",
			t:    TypeInt32,
			v:    func() *int32 { return nil }(),
			exp:  NullValue(TypeInt32),
		},
		{
			name: "uint32",
			t:    TypeUint32,
			v:    func(v uint32) *uint32 { return &v }(123),
			exp:  OptionalValue(Uint32Value(123)),
		},
		{
			name: "nil uint32",
			t:    TypeUint32,
			v:    func() *uint32 { return nil }(),
			exp:  NullValue(TypeUint32),
		},
		{
			name: "int64",
			t:    TypeInt64,
			v:    func(v int64) *int64 { return &v }(123),
			exp:  OptionalValue(Int64Value(123)),
		},
		{
			name: "nil int64",
			t:    TypeInt64,
			v:    func() *int64 { return nil }(),
			exp:  NullValue(TypeInt64),
		},
		{
			name: "uint64",
			t:    TypeUint64,
			v:    func(v uint64) *uint64 { return &v }(123),
			exp:  OptionalValue(Uint64Value(123)),
		},
		{
			name: "nil uint64",
			t:    TypeUint64,
			v:    func() *uint64 { return nil }(),
			exp:  NullValue(TypeUint64),
		},
		{
			name: "float",
			t:    TypeFloat,
			v:    func(v float32) *float32 { return &v }(123),
			exp:  OptionalValue(FloatValue(123)),
		},
		{
			name: "nil float",
			t:    TypeFloat,
			v:    func() *float32 { return nil }(),
			exp:  NullValue(TypeFloat),
		},
		{
			name: "double",
			t:    TypeDouble,
			v:    func(v float64) *float64 { return &v }(123),
			exp:  OptionalValue(DoubleValue(123)),
		},
		{
			name: "nil float",
			t:    TypeDouble,
			v:    func() *float64 { return nil }(),
			exp:  NullValue(TypeDouble),
		},
		{
			name: "date from int32",
			t:    TypeDate,
			v:    func(v uint32) *uint32 { return &v }(123),
			exp:  OptionalValue(DateValue(123)),
		},
		{
			name: "date from time.Time",
			t:    TypeDate,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(DateValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil date",
			t:    TypeDate,
			v:    func() *uint32 { return nil }(),
			exp:  NullValue(TypeDate),
		},
		{
			name: "datetime from int32",
			t:    TypeDatetime,
			v:    func(v uint32) *uint32 { return &v }(123),
			exp:  OptionalValue(DatetimeValue(123)),
		},
		{
			name: "datetime from time.Time",
			t:    TypeDatetime,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(DatetimeValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil datetime",
			t:    TypeDatetime,
			v:    func() *uint32 { return nil }(),
			exp:  NullValue(TypeDatetime),
		},
		{
			name: "timestamp from int32",
			t:    TypeTimestamp,
			v:    func(v uint64) *uint64 { return &v }(123),
			exp:  OptionalValue(TimestampValue(123)),
		},
		{
			name: "timestamp from time.Time",
			t:    TypeTimestamp,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(TimestampValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil timestamp",
			t:    TypeTimestamp,
			v:    func() *uint64 { return nil }(),
			exp:  NullValue(TypeTimestamp),
		},
		{
			name: "tzDate from int32",
			t:    TypeTzDate,
			v:    func(v string) *string { return &v }(""),
			exp:  OptionalValue(TzDateValue("")),
		},
		{
			name: "tzDate from time.Time",
			t:    TypeTzDate,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(TzDateValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil tzDate",
			t:    TypeTzDate,
			v:    func() *string { return nil }(),
			exp:  NullValue(TypeTzDate),
		},
		{
			name: "interval from int64",
			t:    TypeInterval,
			v:    func(v int64) *int64 { return &v }(123),
			exp:  OptionalValue(IntervalValueFromMicroseconds(123)),
		},
		{
			name: "interval from time.Time",
			t:    TypeInterval,
			v:    func(v time.Duration) *time.Duration { return &v }(time.Second),
			exp:  OptionalValue(IntervalValueFromDuration(time.Second)),
		},
		{
			name: "nil interval",
			t:    TypeInterval,
			v:    func() *int64 { return nil }(),
			exp:  NullValue(TypeInterval),
		},
		{
			name: "tzDatetime from int32",
			t:    TypeTzDatetime,
			v:    func(v string) *string { return &v }(""),
			exp:  OptionalValue(TzDatetimeValue("")),
		},
		{
			name: "tzTzDatetime from time.Time",
			t:    TypeTzDatetime,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(TzDatetimeValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil tzTzDatetime",
			t:    TypeTzDatetime,
			v:    func() *string { return nil }(),
			exp:  NullValue(TypeTzDatetime),
		},
		{
			name: "tzTimestamp from int32",
			t:    TypeTzTimestamp,
			v:    func(v string) *string { return &v }(""),
			exp:  OptionalValue(TzTimestampValue("")),
		},
		{
			name: "TzTimestamp from time.Time",
			t:    TypeTzTimestamp,
			v:    func(v time.Time) *time.Time { return &v }(time.Unix(123, 456)),
			exp:  OptionalValue(TzTimestampValueFromTime(time.Unix(123, 456))),
		},
		{
			name: "nil TzTimestamp",
			t:    TypeTzTimestamp,
			v:    func() *string { return nil }(),
			exp:  NullValue(TypeTzTimestamp),
		},
		{
			name: "string",
			t:    TypeString,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(StringValueFromString("test")),
		},
		{
			name: "string",
			t:    TypeString,
			v:    func(v []byte) *[]byte { return &v }([]byte("test")),
			exp:  OptionalValue(StringValueFromString("test")),
		},
		{
			name: "nil string",
			t:    TypeString,
			v:    func() *string { return nil }(),
			exp:  NullValue(TypeString),
		},
		{
			name: "utf8",
			t:    TypeUTF8,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(UTF8Value("test")),
		},
		{
			name: "nil utf8",
			t:    TypeUTF8,
			v:    func() *string { return nil }(),
			exp:  NullValue(TypeUTF8),
		},
		{
			name: "yson",
			t:    TypeYSON,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(YSONValue("test")),
		},
		{
			name: "yson",
			t:    TypeYSON,
			v:    func(v []byte) *[]byte { return &v }([]byte("test")),
			exp:  OptionalValue(YSONValueFromBytes([]byte("test"))),
		},
		{
			name: "nil yson",
			t:    TypeYSON,
			v:    func() *string { return nil }(),
			exp:  NullValue(TypeYSON),
		},
		{
			name: "json",
			t:    TypeJSON,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(JSONValue("test")),
		},
		{
			name: "json",
			t:    TypeJSON,
			v:    func(v []byte) *[]byte { return &v }([]byte("test")),
			exp:  OptionalValue(JSONValueFromBytes([]byte("test"))),
		},
		{
			name: "nil json",
			t:    TypeJSON,
			v:    func() *string { return nil }(),
			exp:  NullValue(TypeJSON),
		},
		{
			name: "uuid",
			t:    TypeUUID,
			v:    func(v [16]byte) *[16]byte { return &v }([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			exp:  OptionalValue(UUIDValue([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
		},
		{
			name: "jsonDocument",
			t:    TypeJSONDocument,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(JSONDocumentValue("test")),
		},
		{
			name: "jsonDocument",
			t:    TypeJSONDocument,
			v:    func(v []byte) *[]byte { return &v }([]byte("test")),
			exp:  OptionalValue(JSONDocumentValueFromBytes([]byte("test"))),
		},
		{
			name: "nil jsonDocument",
			t:    TypeJSONDocument,
			v:    func() *string { return nil }(),
			exp:  NullValue(TypeJSONDocument),
		},
		{
			name: "dyNumber",
			t:    TypeDyNumber,
			v:    func(v string) *string { return &v }("test"),
			exp:  OptionalValue(DyNumberValue("test")),
		},
		{
			name: "nil dyNumber",
			t:    TypeDyNumber,
			v:    func() *string { return nil }(),
			exp:  NullValue(TypeDyNumber),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := allocator.New()
			defer a.Free()
			v := Nullable(test.t, test.v)
			if !proto.Equal(value.ToYDB(v, a), value.ToYDB(test.exp, a)) {
				t.Fatalf("unexpected value: %v, exp: %v", v, test.exp)
			}
		})
	}
}
