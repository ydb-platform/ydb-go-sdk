//go:build !go1.18
// +build !go1.18

package allocator

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type (
	Allocator struct{}
)

func New() (v *Allocator) {
	return &Allocator{}
}

func (a *Allocator) Free() {}

func (a *Allocator) Value() (v *Ydb.Value) {
	return new(Ydb.Value)
}

func (a *Allocator) TypedValue() (v *Ydb.TypedValue) {
	return new(Ydb.TypedValue)
}

func (a *Allocator) Type() (v *Ydb.Type) {
	return new(Ydb.Type)
}

func (a *Allocator) TypePrimitive() (v *Ydb.Type_TypeId) {
	return new(Ydb.Type_TypeId)
}

func (a *Allocator) Decimal() (v *Ydb.DecimalType) {
	return new(Ydb.DecimalType)
}

func (a *Allocator) List() (v *Ydb.ListType) {
	return new(Ydb.ListType)
}

func (a *Allocator) Tuple() (v *Ydb.TupleType) {
	return new(Ydb.TupleType)
}

func (a *Allocator) TypeDecimal() (v *Ydb.Type_DecimalType) {
	return new(Ydb.Type_DecimalType)
}

func (a *Allocator) TypeList() (v *Ydb.Type_ListType) {
	return new(Ydb.Type_ListType)
}

func (a *Allocator) TypeTuple() (v *Ydb.Type_TupleType) {
	return new(Ydb.Type_TupleType)
}

func (a *Allocator) TypeEmptyList() (v *Ydb.Type_EmptyListType) {
	return new(Ydb.Type_EmptyListType)
}

func (a *Allocator) TypeOptional() (v *Ydb.Type_OptionalType) {
	return new(Ydb.Type_OptionalType)
}

func (a *Allocator) Bool() (v *Ydb.Value_BoolValue) {
	return new(Ydb.Value_BoolValue)
}

func (a *Allocator) Bytes() (v *Ydb.Value_BytesValue) {
	return new(Ydb.Value_BytesValue)
}

func (a *Allocator) Int32() (v *Ydb.Value_Int32Value) {
	return new(Ydb.Value_Int32Value)
}

func (a *Allocator) Int64() (v *Ydb.Value_Int64Value) {
	return new(Ydb.Value_Int64Value)
}

func (a *Allocator) Uint32() (v *Ydb.Value_Uint32Value) {
	return new(Ydb.Value_Uint32Value)
}

func (a *Allocator) Float() (v *Ydb.Value_FloatValue) {
	return new(Ydb.Value_FloatValue)
}

func (a *Allocator) Double() (v *Ydb.Value_DoubleValue) {
	return new(Ydb.Value_DoubleValue)
}

func (a *Allocator) Uint64() (v *Ydb.Value_Uint64Value) {
	return new(Ydb.Value_Uint64Value)
}

func (a *Allocator) Text() (v *Ydb.Value_TextValue) {
	return new(Ydb.Value_TextValue)
}

func (a *Allocator) Low128() (v *Ydb.Value_Low_128) {
	return new(Ydb.Value_Low_128)
}

func (a *Allocator) Struct() (v *Ydb.StructType) {
	return new(Ydb.StructType)
}

func (a *Allocator) StructMember() (v *Ydb.StructMember) {
	return new(Ydb.StructMember)
}

func (a *Allocator) Optional() (v *Ydb.OptionalType) {
	return new(Ydb.OptionalType)
}

func (a *Allocator) TypeStruct() (v *Ydb.Type_StructType) {
	return new(Ydb.Type_StructType)
}

func (a *Allocator) Nested() (v *Ydb.Value_NestedValue) {
	return new(Ydb.Value_NestedValue)
}

func (a *Allocator) TypeDict() (v *Ydb.Type_DictType) {
	return new(Ydb.Type_DictType)
}

func (a *Allocator) Dict() (v *Ydb.DictType) {
	return new(Ydb.DictType)
}

func (a *Allocator) Pair() (v *Ydb.ValuePair) {
	return new(Ydb.ValuePair)
}

func (a *Allocator) NullFlag() (v *Ydb.Value_NullFlagValue) {
	return new(Ydb.Value_NullFlagValue)
}

func (a *Allocator) VariantStructItems() (v *Ydb.VariantType_StructItems) {
	return new(Ydb.VariantType_StructItems)
}

func (a *Allocator) TypeVariant() (v *Ydb.Type_VariantType) {
	return new(Ydb.Type_VariantType)
}

func (a *Allocator) Variant() (v *Ydb.VariantType) {
	return new(Ydb.VariantType)
}

func (a *Allocator) VariantTupleItems() (v *Ydb.VariantType_TupleItems) {
	return new(Ydb.VariantType_TupleItems)
}
