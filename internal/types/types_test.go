package types

import (
	"testing"
)

func TestTypeToString(t *testing.T) {
	for _, tt := range []struct {
		t Type
		s string
	}{
		{
			t: NewVoid(),
			s: "Void",
		},
		{
			t: NewNull(),
			s: "Null",
		},
		{
			t: Bool,
			s: "Bool",
		},
		{
			t: Int8,
			s: "Int8",
		},
		{
			t: Uint8,
			s: "Uint8",
		},
		{
			t: Int16,
			s: "Int16",
		},
		{
			t: Uint16,
			s: "Uint16",
		},
		{
			t: Int32,
			s: "Int32",
		},
		{
			t: Uint32,
			s: "Uint32",
		},
		{
			t: Int64,
			s: "Int64",
		},
		{
			t: Uint64,
			s: "Uint64",
		},
		{
			t: Float,
			s: "Float",
		},
		{
			t: Double,
			s: "Double",
		},
		{
			t: Date,
			s: "Date",
		},
		{
			t: Datetime,
			s: "Datetime",
		},
		{
			t: Timestamp,
			s: "Timestamp",
		},
		{
			t: Interval,
			s: "Interval",
		},
		{
			t: TzDate,
			s: "TzDate",
		},
		{
			t: TzDatetime,
			s: "TzDatetime",
		},
		{
			t: TzTimestamp,
			s: "TzTimestamp",
		},
		{
			t: Bytes,
			s: "String",
		},
		{
			t: Text,
			s: "Utf8",
		},
		{
			t: YSON,
			s: "Yson",
		},
		{
			t: JSON,
			s: "Json",
		},
		{
			t: UUID,
			s: "Uuid",
		},
		{
			t: JSONDocument,
			s: "JsonDocument",
		},
		{
			t: DyNumber,
			s: "DyNumber",
		},
		{
			t: NewOptional(Bool),
			s: "Optional<Bool>",
		},
		{
			t: NewOptional(Int8),
			s: "Optional<Int8>",
		},
		{
			t: NewOptional(Uint8),
			s: "Optional<Uint8>",
		},
		{
			t: NewOptional(Int16),
			s: "Optional<Int16>",
		},
		{
			t: NewOptional(Uint16),
			s: "Optional<Uint16>",
		},
		{
			t: NewOptional(Int32),
			s: "Optional<Int32>",
		},
		{
			t: NewOptional(Uint32),
			s: "Optional<Uint32>",
		},
		{
			t: NewOptional(Int64),
			s: "Optional<Int64>",
		},
		{
			t: NewOptional(Uint64),
			s: "Optional<Uint64>",
		},
		{
			t: NewOptional(Float),
			s: "Optional<Float>",
		},
		{
			t: NewOptional(Double),
			s: "Optional<Double>",
		},
		{
			t: NewOptional(Date),
			s: "Optional<Date>",
		},
		{
			t: NewOptional(Datetime),
			s: "Optional<Datetime>",
		},
		{
			t: NewOptional(Timestamp),
			s: "Optional<Timestamp>",
		},
		{
			t: NewOptional(Interval),
			s: "Optional<Interval>",
		},
		{
			t: NewOptional(TzDate),
			s: "Optional<TzDate>",
		},
		{
			t: NewOptional(TzDatetime),
			s: "Optional<TzDatetime>",
		},
		{
			t: NewOptional(TzTimestamp),
			s: "Optional<TzTimestamp>",
		},
		{
			t: NewOptional(Bytes),
			s: "Optional<String>",
		},
		{
			t: NewOptional(Text),
			s: "Optional<Utf8>",
		},
		{
			t: NewOptional(YSON),
			s: "Optional<Yson>",
		},
		{
			t: NewOptional(JSON),
			s: "Optional<Json>",
		},
		{
			t: NewOptional(UUID),
			s: "Optional<Uuid>",
		},
		{
			t: NewOptional(JSONDocument),
			s: "Optional<JsonDocument>",
		},
		{
			t: NewOptional(DyNumber),
			s: "Optional<DyNumber>",
		},
		{
			t: NewDecimal(22, 9),
			s: "Decimal(22,9)",
		},
		{
			t: NewDict(Text, Timestamp),
			s: "Dict<Utf8,Timestamp>",
		},
		{
			t: NewEmptyList(),
			s: "EmptyList",
		},
		{
			t: NewList(Uint32),
			s: "List<Uint32>",
		},
		{
			t: NewSet(Uint32),
			s: "Set<Uint32>",
		},
		{
			t: EmptySet(),
			s: "EmptyDict",
		},
		{
			t: NewEmptyDict(),
			s: "EmptyDict",
		},
		{
			t: NewVariantStruct(
				StructField{
					Name: "a",
					T:    Bool,
				},
				StructField{
					Name: "b",
					T:    Float,
				},
			),
			s: "Variant<'a':Bool,'b':Float>",
		},
		{
			t: NewVariantTuple(
				Bool,
				Float,
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
