package types

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pg"
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
		{
			t: PgType{OID: pg.OIDUnknown},
			s: "PgType(705)",
		},
		{
			t: FromProtobuf(&Ydb.Type{
				Type: &Ydb.Type_VariantType{
					VariantType: &Ydb.VariantType{
						Type: &Ydb.VariantType_StructItems{
							StructItems: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "a",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_BOOL,
											},
										},
									},
									{
										Name: "a",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_FLOAT,
											},
										},
									},
								},
							},
						},
					},
				},
			}),
			s: "Variant<'a':Bool,'a':Float>",
		},
	} {
		t.Run(tt.s, func(t *testing.T) {
			require.Equal(t, tt.s, tt.t.Yql())
		})
	}
}

func TestEqual(t *testing.T) {
	tests := []struct {
		lhs   Type
		rhs   Type
		equal bool
	}{
		{
			Bool,
			Bool,
			true,
		},
		{
			Bool,
			Text,
			false,
		},
		{
			Text,
			Text,
			true,
		},
		{
			NewOptional(Bool),
			NewOptional(Bool),
			true,
		},
		{
			NewOptional(Bool),
			NewOptional(Text),
			false,
		},
		{
			NewOptional(Text),
			NewOptional(Text),
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

func TestOptionalInnerType(t *testing.T) {
	tests := []struct {
		src        Type
		innerType  Type
		isOptional bool
	}{
		{
			Bool,
			nil,
			false,
		},
		{
			Text,
			nil,
			false,
		},
		{
			NewOptional(Bool),
			Bool,
			true,
		},
		{
			NewOptional(Text),
			Text,
			true,
		},
		{
			NewOptional(NewTuple(Text, Bool, Uint64, NewOptional(Int64))),
			NewTuple(Text, Bool, Uint64, NewOptional(Int64)),
			true,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			optional, isOptional := tt.src.(interface {
				IsOptional()
				InnerType() Type
			})
			require.Equal(t, tt.isOptional, isOptional)
			var innerType Type
			if isOptional {
				innerType = optional.InnerType()
			}
			if tt.innerType == nil {
				require.Nil(t, innerType)
			} else {
				require.True(t, Equal(tt.innerType, innerType))
			}
		})
	}
}
