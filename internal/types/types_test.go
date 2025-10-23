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

func TestTypeToYDB(t *testing.T) {
	t.Run("TypeToYDB", func(t *testing.T) {
		typ := Bool
		ydbType := TypeToYDB(typ)
		require.NotNil(t, ydbType)
		require.Equal(t, Ydb.Type_BOOL, ydbType.GetTypeId())
	})
}

func TestDecimal(t *testing.T) {
	t.Run("Precision", func(t *testing.T) {
		d := NewDecimal(22, 9)
		require.Equal(t, uint32(22), d.Precision())
	})
	t.Run("Scale", func(t *testing.T) {
		d := NewDecimal(22, 9)
		require.Equal(t, uint32(9), d.Scale())
	})
	t.Run("String", func(t *testing.T) {
		d := NewDecimal(22, 9)
		require.Equal(t, "Decimal(22,9)", d.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		d1 := NewDecimal(22, 9)
		d2 := NewDecimal(22, 9)
		d3 := NewDecimal(10, 5)

		require.True(t, d1.equalsTo(d2))
		require.False(t, d1.equalsTo(d3))
		require.False(t, d1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		d := NewDecimal(22, 9)
		ydbType := d.ToYDB()
		require.NotNil(t, ydbType)
		require.Equal(t, uint32(22), ydbType.GetDecimalType().GetPrecision())
		require.Equal(t, uint32(9), ydbType.GetDecimalType().GetScale())
	})
}

func TestDict(t *testing.T) {
	t.Run("KeyType", func(t *testing.T) {
		d := NewDict(Text, Int32)
		require.True(t, Equal(Text, d.KeyType()))
	})
	t.Run("ValueType", func(t *testing.T) {
		d := NewDict(Text, Int32)
		require.True(t, Equal(Int32, d.ValueType()))
	})
	t.Run("String", func(t *testing.T) {
		d := NewDict(Text, Int32)
		require.Equal(t, "Dict<Utf8,Int32>", d.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		d1 := NewDict(Text, Int32)
		d2 := NewDict(Text, Int32)
		d3 := NewDict(Text, Int64)
		d4 := NewDict(Bytes, Int32)

		require.True(t, d1.equalsTo(d2))
		require.False(t, d1.equalsTo(d3))
		require.False(t, d1.equalsTo(d4))
		require.False(t, d1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		d := NewDict(Text, Int32)
		ydbType := d.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetDictType())
		require.Equal(t, Ydb.Type_UTF8, ydbType.GetDictType().GetKey().GetTypeId())
		require.Equal(t, Ydb.Type_INT32, ydbType.GetDictType().GetPayload().GetTypeId())
	})
}

func TestEmptyList(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		el := NewEmptyList()
		require.Equal(t, "EmptyList", el.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		el1 := NewEmptyList()
		el2 := NewEmptyList()
		require.True(t, el1.equalsTo(el2))
		require.False(t, el1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		el := NewEmptyList()
		ydbType := el.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetEmptyListType())
	})
}

func TestEmptyDict(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		ed := NewEmptyDict()
		require.Equal(t, "EmptyDict", ed.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		ed1 := NewEmptyDict()
		ed2 := NewEmptyDict()
		require.True(t, ed1.equalsTo(ed2))
		require.False(t, ed1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		ed := NewEmptyDict()
		ydbType := ed.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetEmptyDictType())
	})
}

func TestList(t *testing.T) {
	t.Run("ItemType", func(t *testing.T) {
		l := NewList(Int32)
		require.True(t, Equal(Int32, l.ItemType()))
	})
	t.Run("String", func(t *testing.T) {
		l := NewList(Int32)
		require.Equal(t, "List<Int32>", l.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		l1 := NewList(Int32)
		l2 := NewList(Int32)
		l3 := NewList(Int64)

		require.True(t, l1.equalsTo(l2))
		require.False(t, l1.equalsTo(l3))
		require.False(t, l1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		l := NewList(Int32)
		ydbType := l.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetListType())
		require.Equal(t, Ydb.Type_INT32, ydbType.GetListType().GetItem().GetTypeId())
	})
}

func TestSet(t *testing.T) {
	t.Run("ItemType", func(t *testing.T) {
		s := NewSet(Int32)
		require.True(t, Equal(Int32, s.ItemType()))
	})
	t.Run("String", func(t *testing.T) {
		s := NewSet(Int32)
		require.Equal(t, "Set<Int32>", s.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		s1 := NewSet(Int32)
		s2 := NewSet(Int32)
		s3 := NewSet(Int64)

		require.True(t, s1.equalsTo(s2))
		require.False(t, s1.equalsTo(s3))
		require.False(t, s1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		s := NewSet(Int32)
		ydbType := s.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetDictType())
		require.Equal(t, Ydb.Type_INT32, ydbType.GetDictType().GetKey().GetTypeId())
	})
}

func TestOptional(t *testing.T) {
	t.Run("IsOptional", func(t *testing.T) {
		o := NewOptional(Bool)
		o.IsOptional()
	})
	t.Run("String", func(t *testing.T) {
		o := NewOptional(Bool)
		require.Equal(t, "Optional<Bool>", o.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		o1 := NewOptional(Bool)
		o2 := NewOptional(Bool)
		o3 := NewOptional(Text)

		require.True(t, o1.equalsTo(o2))
		require.False(t, o1.equalsTo(o3))
		require.False(t, o1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		o := NewOptional(Bool)
		ydbType := o.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetOptionalType())
		require.Equal(t, Ydb.Type_BOOL, ydbType.GetOptionalType().GetItem().GetTypeId())
	})
}

func TestPgType(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		pg := PgType{OID: 123}
		require.Equal(t, "PgType(123)", pg.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		pg1 := PgType{OID: 123}
		pg2 := PgType{OID: 123}
		pg3 := PgType{OID: 456}

		require.True(t, pg1.equalsTo(pg2))
		require.False(t, pg1.equalsTo(pg3))
		require.False(t, pg1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		pg := PgType{OID: 123}
		ydbType := pg.ToYDB()
		require.NotNil(t, ydbType)
		require.Equal(t, uint32(123), ydbType.GetPgType().GetOid())
	})
}

func TestPrimitive(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		require.Equal(t, "Bool", Bool.String())
		require.Equal(t, "Int32", Int32.String())
		require.Equal(t, "Utf8", Text.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		require.True(t, Bool.equalsTo(Bool))
		require.False(t, Bool.equalsTo(Text))
		require.False(t, Bool.equalsTo(NewOptional(Bool)))
	})
	t.Run("ToYDB", func(t *testing.T) {
		ydbType := Bool.ToYDB()
		require.NotNil(t, ydbType)
		require.Equal(t, Ydb.Type_BOOL, ydbType.GetTypeId())
	})
}

func TestStruct(t *testing.T) {
	t.Run("Field", func(t *testing.T) {
		s := NewStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		require.Equal(t, "a", s.Field(0).Name)
		require.True(t, Equal(Bool, s.Field(0).T))
		require.Equal(t, "b", s.Field(1).Name)
		require.True(t, Equal(Int32, s.Field(1).T))
	})
	t.Run("Fields", func(t *testing.T) {
		s := NewStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		fields := s.Fields()
		require.Len(t, fields, 2)
		require.Equal(t, "a", fields[0].Name)
		require.Equal(t, "b", fields[1].Name)
	})
	t.Run("String", func(t *testing.T) {
		s := NewStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		require.Equal(t, "Struct<'a':Bool,'b':Int32>", s.String())
	})
	t.Run("Yql", func(t *testing.T) {
		s := NewStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		require.Equal(t, "Struct<'a':Bool,'b':Int32>", s.Yql())
	})
	t.Run("equalsTo", func(t *testing.T) {
		s1 := NewStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		s2 := NewStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		s3 := NewStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int64},
		)
		s4 := NewStruct(
			StructField{Name: "x", T: Bool},
			StructField{Name: "y", T: Int32},
		)
		s5 := NewStruct(
			StructField{Name: "a", T: Bool},
		)

		require.True(t, s1.equalsTo(s2))
		require.False(t, s1.equalsTo(s3))
		require.False(t, s1.equalsTo(s4))
		require.False(t, s1.equalsTo(s5))
		require.False(t, s1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		s := NewStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		ydbType := s.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetStructType())
		require.Len(t, ydbType.GetStructType().GetMembers(), 2)
		require.Equal(t, "a", ydbType.GetStructType().GetMembers()[0].GetName())
		require.Equal(t, "b", ydbType.GetStructType().GetMembers()[1].GetName())
	})
}

func TestTuple(t *testing.T) {
	t.Run("InnerTypes", func(t *testing.T) {
		tup := NewTuple(Bool, Int32, Text)
		types := tup.InnerTypes()
		require.Len(t, types, 3)
		require.True(t, Equal(Bool, types[0]))
		require.True(t, Equal(Int32, types[1]))
		require.True(t, Equal(Text, types[2]))
	})
	t.Run("ItemType", func(t *testing.T) {
		tup := NewTuple(Bool, Int32, Text)
		require.True(t, Equal(Bool, tup.ItemType(0)))
		require.True(t, Equal(Int32, tup.ItemType(1)))
		require.True(t, Equal(Text, tup.ItemType(2)))
	})
	t.Run("String", func(t *testing.T) {
		tup := NewTuple(Bool, Int32)
		require.Equal(t, "Tuple<Bool,Int32>", tup.String())
	})
	t.Run("Yql", func(t *testing.T) {
		tup := NewTuple(Bool, Int32)
		require.Equal(t, "Tuple<Bool,Int32>", tup.Yql())
	})
	t.Run("equalsTo", func(t *testing.T) {
		tup1 := NewTuple(Bool, Int32)
		tup2 := NewTuple(Bool, Int32)
		tup3 := NewTuple(Bool, Int64)
		tup4 := NewTuple(Bool, Int32, Text)

		require.True(t, tup1.equalsTo(tup2))
		require.False(t, tup1.equalsTo(tup3))
		require.False(t, tup1.equalsTo(tup4))
		require.False(t, tup1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		tup := NewTuple(Bool, Int32)
		ydbType := tup.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetTupleType())
		require.Len(t, ydbType.GetTupleType().GetElements(), 2)
	})
}

func TestVariantStruct(t *testing.T) {
	t.Run("equalsTo", func(t *testing.T) {
		vs1 := NewVariantStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		vs2 := NewVariantStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		vs3 := NewVariantStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int64},
		)
		s := NewStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)

		require.True(t, vs1.equalsTo(vs2))
		require.False(t, vs1.equalsTo(vs3))
		require.True(t, vs1.equalsTo(s))
		require.False(t, vs1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		vs := NewVariantStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		ydbType := vs.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetVariantType())
		require.NotNil(t, ydbType.GetVariantType().GetStructItems())
	})
}

func TestVariantTuple(t *testing.T) {
	t.Run("equalsTo", func(t *testing.T) {
		vt1 := NewVariantTuple(Bool, Int32)
		vt2 := NewVariantTuple(Bool, Int32)
		vt3 := NewVariantTuple(Bool, Int64)
		tup := NewTuple(Bool, Int32)

		require.True(t, vt1.equalsTo(vt2))
		require.False(t, vt1.equalsTo(vt3))
		require.True(t, vt1.equalsTo(tup))
		require.False(t, vt1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		vt := NewVariantTuple(Bool, Int32)
		ydbType := vt.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetVariantType())
		require.NotNil(t, ydbType.GetVariantType().GetTupleItems())
	})
}

func TestVoid(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		v := NewVoid()
		require.Equal(t, "Void", v.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		v1 := NewVoid()
		v2 := NewVoid()
		require.True(t, v1.equalsTo(v2))
		require.False(t, v1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		v := NewVoid()
		ydbType := v.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetVoidType())
	})
}

func TestNull(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		n := NewNull()
		require.Equal(t, "Null", n.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		n1 := NewNull()
		n2 := NewNull()
		require.True(t, n1.equalsTo(n2))
		require.False(t, n1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		n := NewNull()
		ydbType := n.ToYDB()
		require.NotNil(t, ydbType)
		require.NotNil(t, ydbType.GetNullType())
	})
}

func TestProtobufType(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		pb := FromProtobuf(&Ydb.Type{
			Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL},
		})
		require.Equal(t, "Bool", pb.String())
	})
	t.Run("equalsTo", func(t *testing.T) {
		pb1 := FromProtobuf(&Ydb.Type{
			Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL},
		})
		pb2 := FromProtobuf(&Ydb.Type{
			Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL},
		})
		pb3 := FromProtobuf(&Ydb.Type{
			Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32},
		})

		require.True(t, pb1.equalsTo(pb2))
		require.False(t, pb1.equalsTo(pb3))
		require.False(t, pb1.equalsTo(Bool))
	})
	t.Run("ToYDB", func(t *testing.T) {
		ydbInput := &Ydb.Type{
			Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL},
		}
		pb := FromProtobuf(ydbInput)
		ydbType := pb.ToYDB()
		require.NotNil(t, ydbType)
		require.Equal(t, Ydb.Type_BOOL, ydbType.GetTypeId())
	})
}

func TestTypeFromYDB(t *testing.T) {
	t.Run("PrimitiveTypes", func(t *testing.T) {
		primitives := []struct {
			ydbType Ydb.Type_PrimitiveTypeId
			goType  Type
		}{
			{Ydb.Type_BOOL, Bool},
			{Ydb.Type_INT8, Int8},
			{Ydb.Type_UINT8, Uint8},
			{Ydb.Type_INT16, Int16},
			{Ydb.Type_UINT16, Uint16},
			{Ydb.Type_INT32, Int32},
			{Ydb.Type_UINT32, Uint32},
			{Ydb.Type_INT64, Int64},
			{Ydb.Type_UINT64, Uint64},
			{Ydb.Type_FLOAT, Float},
			{Ydb.Type_DOUBLE, Double},
			{Ydb.Type_DATE, Date},
			{Ydb.Type_DATE32, Date32},
			{Ydb.Type_DATETIME, Datetime},
			{Ydb.Type_DATETIME64, Datetime64},
			{Ydb.Type_TIMESTAMP, Timestamp},
			{Ydb.Type_TIMESTAMP64, Timestamp64},
			{Ydb.Type_INTERVAL, Interval},
			{Ydb.Type_INTERVAL64, Interval64},
			{Ydb.Type_TZ_DATE, TzDate},
			{Ydb.Type_TZ_DATETIME, TzDatetime},
			{Ydb.Type_TZ_TIMESTAMP, TzTimestamp},
			{Ydb.Type_STRING, Bytes},
			{Ydb.Type_UTF8, Text},
			{Ydb.Type_YSON, YSON},
			{Ydb.Type_JSON, JSON},
			{Ydb.Type_UUID, UUID},
			{Ydb.Type_JSON_DOCUMENT, JSONDocument},
			{Ydb.Type_DYNUMBER, DyNumber},
		}
		for _, p := range primitives {
			t.Run(p.goType.Yql(), func(t *testing.T) {
				ydbType := &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: p.ydbType},
				}
				goType := TypeFromYDB(ydbType)
				require.True(t, Equal(p.goType, goType))
			})
		}
	})
	t.Run("OptionalType", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_OptionalType{
				OptionalType: &Ydb.OptionalType{
					Item: &Ydb.Type{
						Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL},
					},
				},
			},
		}
		goType := TypeFromYDB(ydbType)
		require.True(t, Equal(NewOptional(Bool), goType))
	})
	t.Run("ListType", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_ListType{
				ListType: &Ydb.ListType{
					Item: &Ydb.Type{
						Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32},
					},
				},
			},
		}
		goType := TypeFromYDB(ydbType)
		require.True(t, Equal(NewList(Int32), goType))
	})
	t.Run("DecimalType", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_DecimalType{
				DecimalType: &Ydb.DecimalType{
					Precision: 22,
					Scale:     9,
				},
			},
		}
		goType := TypeFromYDB(ydbType)
		require.True(t, Equal(NewDecimal(22, 9), goType))
	})
	t.Run("TupleType", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_TupleType{
				TupleType: &Ydb.TupleType{
					Elements: []*Ydb.Type{
						{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}},
						{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
					},
				},
			},
		}
		goType := TypeFromYDB(ydbType)
		require.True(t, Equal(NewTuple(Bool, Int32), goType))
	})
	t.Run("StructType", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_StructType{
				StructType: &Ydb.StructType{
					Members: []*Ydb.StructMember{
						{Name: "a", Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}}},
						{Name: "b", Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}}},
					},
				},
			},
		}
		goType := TypeFromYDB(ydbType)
		expected := NewStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		require.True(t, Equal(expected, goType))
	})
	t.Run("DictType", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_DictType{
				DictType: &Ydb.DictType{
					Key:     &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
					Payload: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
				},
			},
		}
		goType := TypeFromYDB(ydbType)
		require.True(t, Equal(NewDict(Text, Int32), goType))
	})
	t.Run("DictTypeAsSet", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_DictType{
				DictType: &Ydb.DictType{
					Key:     &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
					Payload: &Ydb.Type{Type: &Ydb.Type_VoidType{}},
				},
			},
		}
		goType := TypeFromYDB(ydbType)
		require.True(t, Equal(NewSet(Text), goType))
	})
	t.Run("VariantTypeTuple", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_VariantType{
				VariantType: &Ydb.VariantType{
					Type: &Ydb.VariantType_TupleItems{
						TupleItems: &Ydb.TupleType{
							Elements: []*Ydb.Type{
								{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}},
								{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
							},
						},
					},
				},
			},
		}
		goType := TypeFromYDB(ydbType)
		require.True(t, Equal(NewVariantTuple(Bool, Int32), goType))
	})
	t.Run("VariantTypeStruct", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_VariantType{
				VariantType: &Ydb.VariantType{
					Type: &Ydb.VariantType_StructItems{
						StructItems: &Ydb.StructType{
							Members: []*Ydb.StructMember{
								{Name: "a", Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}}},
								{Name: "b", Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}}},
							},
						},
					},
				},
			},
		}
		goType := TypeFromYDB(ydbType)
		expected := NewVariantStruct(
			StructField{Name: "a", T: Bool},
			StructField{Name: "b", T: Int32},
		)
		require.True(t, Equal(expected, goType))
	})
	t.Run("VoidType", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_VoidType{},
		}
		goType := TypeFromYDB(ydbType)
		require.True(t, Equal(NewVoid(), goType))
	})
	t.Run("NullType", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_NullType{},
		}
		goType := TypeFromYDB(ydbType)
		require.True(t, Equal(NewNull(), goType))
	})
	t.Run("PgType", func(t *testing.T) {
		ydbType := &Ydb.Type{
			Type: &Ydb.Type_PgType{
				PgType: &Ydb.PgType{
					Oid: 123,
				},
			},
		}
		goType := TypeFromYDB(ydbType)
		require.True(t, Equal(goType, PgType{OID: 123}))
	})
}
