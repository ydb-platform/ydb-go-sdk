package value

import (
	"testing"
)

func TestTypeToString(t *testing.T) {
	for _, tt := range []struct {
		t Type
		s string
	}{
		{
			t: Void(),
			s: "Void",
		},
		{
			t: Null(),
			s: "Null",
		},
		{
			t: TypeBool,
			s: "Bool",
		},
		{
			t: TypeInt8,
			s: "Int8",
		},
		{
			t: TypeUint8,
			s: "Uint8",
		},
		{
			t: TypeInt16,
			s: "Int16",
		},
		{
			t: TypeUint16,
			s: "Uint16",
		},
		{
			t: TypeInt32,
			s: "Int32",
		},
		{
			t: TypeUint32,
			s: "Uint32",
		},
		{
			t: TypeInt64,
			s: "Int64",
		},
		{
			t: TypeUint64,
			s: "Uint64",
		},
		{
			t: TypeFloat,
			s: "Float",
		},
		{
			t: TypeDouble,
			s: "Double",
		},
		{
			t: TypeDate,
			s: "Date",
		},
		{
			t: TypeDatetime,
			s: "Datetime",
		},
		{
			t: TypeTimestamp,
			s: "Timestamp",
		},
		{
			t: TypeInterval,
			s: "Interval",
		},
		{
			t: TypeTzDate,
			s: "TzDate",
		},
		{
			t: TypeTzDatetime,
			s: "TzDatetime",
		},
		{
			t: TypeTzTimestamp,
			s: "TzTimestamp",
		},
		{
			t: TypeBytes,
			s: "String",
		},
		{
			t: TypeText,
			s: "Utf8",
		},
		{
			t: TypeYSON,
			s: "Yson",
		},
		{
			t: TypeJSON,
			s: "Json",
		},
		{
			t: TypeUUID,
			s: "Uuid",
		},
		{
			t: TypeJSONDocument,
			s: "JsonDocument",
		},
		{
			t: TypeDyNumber,
			s: "DyNumber",
		},
		{
			t: Optional(TypeBool),
			s: "Optional<Bool>",
		},
		{
			t: Optional(TypeInt8),
			s: "Optional<Int8>",
		},
		{
			t: Optional(TypeUint8),
			s: "Optional<Uint8>",
		},
		{
			t: Optional(TypeInt16),
			s: "Optional<Int16>",
		},
		{
			t: Optional(TypeUint16),
			s: "Optional<Uint16>",
		},
		{
			t: Optional(TypeInt32),
			s: "Optional<Int32>",
		},
		{
			t: Optional(TypeUint32),
			s: "Optional<Uint32>",
		},
		{
			t: Optional(TypeInt64),
			s: "Optional<Int64>",
		},
		{
			t: Optional(TypeUint64),
			s: "Optional<Uint64>",
		},
		{
			t: Optional(TypeFloat),
			s: "Optional<Float>",
		},
		{
			t: Optional(TypeDouble),
			s: "Optional<Double>",
		},
		{
			t: Optional(TypeDate),
			s: "Optional<Date>",
		},
		{
			t: Optional(TypeDatetime),
			s: "Optional<Datetime>",
		},
		{
			t: Optional(TypeTimestamp),
			s: "Optional<Timestamp>",
		},
		{
			t: Optional(TypeInterval),
			s: "Optional<Interval>",
		},
		{
			t: Optional(TypeTzDate),
			s: "Optional<TzDate>",
		},
		{
			t: Optional(TypeTzDatetime),
			s: "Optional<TzDatetime>",
		},
		{
			t: Optional(TypeTzTimestamp),
			s: "Optional<TzTimestamp>",
		},
		{
			t: Optional(TypeBytes),
			s: "Optional<String>",
		},
		{
			t: Optional(TypeText),
			s: "Optional<Utf8>",
		},
		{
			t: Optional(TypeYSON),
			s: "Optional<Yson>",
		},
		{
			t: Optional(TypeJSON),
			s: "Optional<Json>",
		},
		{
			t: Optional(TypeUUID),
			s: "Optional<Uuid>",
		},
		{
			t: Optional(TypeJSONDocument),
			s: "Optional<JsonDocument>",
		},
		{
			t: Optional(TypeDyNumber),
			s: "Optional<DyNumber>",
		},
		{
			t: Decimal(22, 9),
			s: "Decimal(22,9)",
		},
		{
			t: Dict(TypeText, TypeTimestamp),
			s: "Dict<Utf8,Timestamp>",
		},
		{
			t: EmptyList(),
			s: "EmptyList",
		},
		{
			t: List(TypeUint32),
			s: "List<Uint32>",
		},
		{
			t: Set(TypeUint32),
			s: "Set<Uint32>",
		},
		{
			t: EmptySet(),
			s: "EmptyDict",
		},
		{
			t: EmptyDict(),
			s: "EmptyDict",
		},
		{
			t: VariantStruct(
				StructField{
					Name: "a",
					T:    TypeBool,
				},
				StructField{
					Name: "b",
					T:    TypeFloat,
				},
			),
			s: "Variant<'a':Bool,'b':Float>",
		},
		{
			t: VariantTuple(
				TypeBool,
				TypeFloat,
			),
			s: "Variant<Bool,Float>",
		},
	} {
		t.Run(tt.s, func(t *testing.T) {
			if got := tt.t.Yql(); got != tt.s {
				t.Errorf("s representations not equals:\n\n -  got: %s\n\n - want: %s", got, tt.s)
			}
		})
	}
}
