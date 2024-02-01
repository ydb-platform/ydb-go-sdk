package value

import (
	"testing"
)

func TestTypeToString(t *testing.T) {
	var testCases []struct {
		t Type
		s string
	}

	testCases = append(testCases, getNumericalTypeTestCases()...)
	testCases = append(testCases, getDateAndTimeTypeTestCases()...)
	testCases = append(testCases, getStringAndTextTypeTestCases()...)
	testCases = append(testCases, getMiscellaneousTypeTestCases()...)

	for _, tt := range testCases {
		t.Run(tt.s, func(t *testing.T) {
			if got := tt.t.Yql(); got != tt.s {
				t.Errorf("s representations not equals:\n\n -  got: %s\n\n - want: %s", got, tt.s)
			}
		})
	}
}

func getNumericalTypeTestCases() []struct {
	t Type
	s string
} {
	return []struct {
		t Type
		s string
	}{
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
			t: Decimal(22, 9),
			s: "Decimal(22,9)",
		},
	}
}
func getDateAndTimeTypeTestCases() []struct {
	t Type
	s string
} {
	return []struct {
		t Type
		s string
	}{
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
	}
}

func getStringAndTextTypeTestCases() []struct {
	t Type
	s string
} {
	return []struct {
		t Type
		s string
	}{
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
	}
}
func getMiscellaneousTypeTestCases() []struct {
	t Type
	s string
} {
	return []struct {
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
			t: Optional(TypeBool),
			s: "Optional<Bool>",
		},
		{
			t: Optional(TypeJSON),
			s: "Optional<Json>",
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
			t: EmptyList(),
			s: "EmptyList",
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
		{
			t: Dict(TypeText, TypeTimestamp),
			s: "Dict<Utf8,Timestamp>",
		},
	}
}
