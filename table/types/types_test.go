package types

import (
	"bytes"
	"fmt"
	"testing"
)

func TestEqual(t *testing.T) {
	tests := []struct {
		lhs   Type
		rhs   Type
		equal bool
	}{
		{
			TypeBool,
			TypeBool,
			true,
		},
		{
			TypeBool,
			TypeUTF8,
			false,
		},
		{
			TypeUTF8,
			TypeUTF8,
			true,
		},
		{
			Optional(TypeBool),
			Optional(TypeBool),
			true,
		},
		{
			Optional(TypeBool),
			Optional(TypeUTF8),
			false,
		},
		{
			Optional(TypeUTF8),
			Optional(TypeUTF8),
			true,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if equal := Equal(tt.lhs, tt.rhs); equal != tt.equal {
				t.Errorf("Equal(%s, %s) = %v, want %v", tt.lhs, tt.rhs, equal, tt.equal)
			}
		})
	}
}

func TestWriteTypeStringTo(t *testing.T) {
	for _, tt := range []struct {
		t Type
		s string
	}{
		{
			t: TypeBool,
			s: "Bool",
		},
		{
			t: Optional(TypeBool),
			s: "Optional<Bool>",
		},
		{
			t: TypeInt8,
			s: "Int8",
		},
		{
			t: Optional(TypeInt8),
			s: "Optional<Int8>",
		},
		{
			t: TypeUint8,
			s: "Uint8",
		},
		{
			t: Optional(TypeUint8),
			s: "Optional<Uint8>",
		},
		{
			t: TypeInt16,
			s: "Int16",
		},
		{
			t: Optional(TypeInt16),
			s: "Optional<Int16>",
		},
		{
			t: TypeUint16,
			s: "Uint16",
		},
		{
			t: Optional(TypeUint16),
			s: "Optional<Uint16>",
		},
		{
			t: TypeInt32,
			s: "Int32",
		},
		{
			t: Optional(TypeInt32),
			s: "Optional<Int32>",
		},
		{
			t: TypeUint32,
			s: "Uint32",
		},
		{
			t: Optional(TypeUint32),
			s: "Optional<Uint32>",
		},
		{
			t: TypeInt64,
			s: "Int64",
		},
		{
			t: Optional(TypeInt64),
			s: "Optional<Int64>",
		},
		{
			t: TypeUint64,
			s: "Uint64",
		},
		{
			t: Optional(TypeUint64),
			s: "Optional<Uint64>",
		},
		{
			t: TypeFloat,
			s: "Float",
		},
		{
			t: Optional(TypeFloat),
			s: "Optional<Float>",
		},
		{
			t: TypeDouble,
			s: "Double",
		},
		{
			t: Optional(TypeDouble),
			s: "Optional<Double>",
		},
		{
			t: TypeDate,
			s: "Date",
		},
		{
			t: Optional(TypeDate),
			s: "Optional<Date>",
		},
		{
			t: TypeDatetime,
			s: "Datetime",
		},
		{
			t: Optional(TypeDatetime),
			s: "Optional<Datetime>",
		},
		{
			t: TypeTimestamp,
			s: "Timestamp",
		},
		{
			t: Optional(TypeTimestamp),
			s: "Optional<Timestamp>",
		},
		{
			t: TypeInterval,
			s: "Interval",
		},
		{
			t: Optional(TypeInterval),
			s: "Optional<Interval>",
		},
		{
			t: TypeTzDate,
			s: "TzDate",
		},
		{
			t: Optional(TypeTzDate),
			s: "Optional<TzDate>",
		},
		{
			t: TypeTzDatetime,
			s: "TzDatetime",
		},
		{
			t: Optional(TypeTzDatetime),
			s: "Optional<TzDatetime>",
		},
		{
			t: TypeTzTimestamp,
			s: "TzTimestamp",
		},
		{
			t: Optional(TypeTzTimestamp),
			s: "Optional<TzTimestamp>",
		},
		{
			t: TypeBytes,
			s: "Bytes",
		},
		{
			t: Optional(TypeString),
			s: "Optional<Bytes>",
		},
		{
			t: TypeText,
			s: "Text",
		},
		{
			t: Optional(TypeText),
			s: "Optional<Text>",
		},
		{
			t: TypeYSON,
			s: "Yson",
		},
		{
			t: Optional(TypeYSON),
			s: "Optional<Yson>",
		},
		{
			t: TypeJSON,
			s: "Json",
		},
		{
			t: Optional(TypeJSON),
			s: "Optional<Json>",
		},
		{
			t: TypeUUID,
			s: "Uuid",
		},
		{
			t: Optional(TypeUUID),
			s: "Optional<Uuid>",
		},
		{
			t: TypeJSONDocument,
			s: "JsonDocument",
		},
		{
			t: Optional(TypeJSONDocument),
			s: "Optional<JsonDocument>",
		},
		{
			t: TypeDyNumber,
			s: "DyNumber",
		},
		{
			t: Optional(TypeDyNumber),
			s: "Optional<DyNumber>",
		},
		{
			t: List(TypeInt64),
			s: "List<Int64>",
		},
		{
			t: Struct(
				StructField("series_id", TypeUint64),
				StructField("title", TypeUTF8),
				StructField("air_date", TypeDate),
				StructField("remove_date", Optional(TypeTzDatetime)),
			),
			s: "Struct<series_id:Uint64,title:Text,air_date:Date,remove_date:Optional<TzDatetime>>",
		},
		{
			t: Dict(TypeUTF8, Optional(TypeTzDatetime)),
			s: "Dict<Text,Optional<TzDatetime>>",
		},
		{
			t: Tuple(TypeUTF8, List(TypeInt64), Optional(TypeTzDatetime)),
			s: "Tuple<Text,List<Int64>,Optional<TzDatetime>>",
		},
		{
			t: Variant(Tuple(TypeUTF8, List(TypeInt64), Optional(TypeTzDatetime))),
			s: "Variant<Tuple<Text,List<Int64>,Optional<TzDatetime>>>",
		},
		{
			t: Variant(Struct(
				StructField("series_id", TypeUint64),
				StructField("title", TypeUTF8),
				StructField("air_date", TypeDate),
				StructField("remove_date", Optional(TypeTzDatetime)),
			)),
			s: "Variant<Struct<series_id:Uint64,title:Text,air_date:Date,remove_date:Optional<TzDatetime>>>",
		},
	} {
		t.Run(fmt.Sprintf("%+v", tt.t), func(t *testing.T) {
			var buf bytes.Buffer
			WriteTypeStringTo(&buf, tt.t)
			if buf.String() != tt.s {
				t.Fatalf("unexpected string representation of %+v.\n\ngot: %s\n\nexp: %s", tt.t, buf.String(), tt.s)
			}
		})
	}
}
