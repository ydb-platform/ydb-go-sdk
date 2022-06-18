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

func (a *Allocator) EmptyTypeList() (v *Ydb.Type_EmptyListType) {
	return new(Ydb.Type_EmptyListType)
}

func (a *Allocator) TypeOptional() (v *Ydb.Type_OptionalType) {
	return new(Ydb.Type_OptionalType)
}

func (a *Allocator) BoolValue() (v *Ydb.Value_BoolValue) {
	return new(Ydb.Value_BoolValue)
}

func (a *Allocator) BytesValue() (v *Ydb.Value_BytesValue) {
	return new(Ydb.Value_BytesValue)
}

func (a *Allocator) Int32Value() (v *Ydb.Value_Int32Value) {
	return new(Ydb.Value_Int32Value)
}

func (a *Allocator) Int64Value() (v *Ydb.Value_Int64Value) {
	return new(Ydb.Value_Int64Value)
}

func (a *Allocator) Uint32Value() (v *Ydb.Value_Uint32Value) {
	return new(Ydb.Value_Uint32Value)
}

func (a *Allocator) FloatValue() (v *Ydb.Value_FloatValue) {
	return new(Ydb.Value_FloatValue)
}

func (a *Allocator) DoubleValue() (v *Ydb.Value_DoubleValue) {
	return new(Ydb.Value_DoubleValue)
}

func (a *Allocator) Uint64Value() (v *Ydb.Value_Uint64Value) {
	return new(Ydb.Value_Uint64Value)
}

func (a *Allocator) TextValue() (v *Ydb.Value_TextValue) {
	return new(Ydb.Value_TextValue)
}

func (a *Allocator) Low128Value() (v *Ydb.Value_Low_128) {
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

func (a *Allocator) NestedValue() (v *Ydb.Value_NestedValue) {
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
