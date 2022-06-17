// go:build +go1.18

package allocator

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"sync"
)

type (
	Allocator struct {
		valueAllocations         []*Ydb.Value
		typeAllocations          []*Ydb.Type
		typePrimitiveAllocations []*Ydb.Type_TypeId
		typeDecimalAllocations   []*Ydb.Type_DecimalType
		typeListAllocations      []*Ydb.Type_ListType
		typeEmptyListAllocations []*Ydb.Type_EmptyListType
		typeTupleAllocations     []*Ydb.Type_TupleType
		decimalAllocations       []*Ydb.DecimalType
		listAllocations          []*Ydb.ListType
		tupleAllocations         []*Ydb.TupleType
		typeOptionalAllocations  []*Ydb.Type_OptionalType
		typedValueAllocations    []*Ydb.TypedValue
		boolAllocations          []*Ydb.Value_BoolValue
		bytesAllocations         []*Ydb.Value_BytesValue
		textAllocations          []*Ydb.Value_TextValue
		int32Allocations         []*Ydb.Value_Int32Value
		uint32Allocations        []*Ydb.Value_Uint32Value
		low128Allocations        []*Ydb.Value_Low_128
		int64Allocations         []*Ydb.Value_Int64Value
		uint64Allocations        []*Ydb.Value_Uint64Value
		floatAllocations         []*Ydb.Value_FloatValue
		doubleAllocations        []*Ydb.Value_DoubleValue
	}
	pool[T any] sync.Pool
)

func (p *pool[T]) Get() *T {
	v := (*sync.Pool)(p).Get()
	if v == nil {
		var zero T
		v = &zero
	}
	if vv, ok := (v).(interface {
		Reset()
	}); ok {
		vv.Reset()
	}
	return v.(*T)
}

func (p *pool[T]) Put(t *T) {
	(*sync.Pool)(p).Put(t)
}

var (
	allocatorsPool    pool[Allocator]
	valuePool         pool[Ydb.Value]
	typePool          pool[Ydb.Type]
	typePrimitivePool pool[Ydb.Type_TypeId]
	typeDecimalPool   pool[Ydb.Type_DecimalType]
	typeListPool      pool[Ydb.Type_ListType]
	typeEmptyListPool pool[Ydb.Type_EmptyListType]
	typeTuplePool     pool[Ydb.Type_TupleType]
	decimalPool       pool[Ydb.DecimalType]
	listPool          pool[Ydb.ListType]
	tuplePool         pool[Ydb.TupleType]
	typeOptionalPool  pool[Ydb.Type_OptionalType]
	typedValuePool    pool[Ydb.TypedValue]
	boolPool          pool[Ydb.Value_BoolValue]
	bytesPool         pool[Ydb.Value_BytesValue]
	textPool          pool[Ydb.Value_TextValue]
	int32Pool         pool[Ydb.Value_Int32Value]
	uint32Pool        pool[Ydb.Value_Uint32Value]
	low128Pool        pool[Ydb.Value_Low_128]
	int64Pool         pool[Ydb.Value_Int64Value]
	uint64Pool        pool[Ydb.Value_Uint64Value]
	floatPool         pool[Ydb.Value_FloatValue]
	doublePool        pool[Ydb.Value_DoubleValue]
)

func New() (v *Allocator) {
	return allocatorsPool.Get()
}

func (a *Allocator) Free() {
	for _, v := range a.valueAllocations {
		v.Reset()
		valuePool.Put(v)
	}
	a.valueAllocations = a.valueAllocations[:0]
	for _, v := range a.typeAllocations {
		v.Reset()
		typePool.Put(v)
	}
	a.typeAllocations = a.typeAllocations[:0]
	for _, v := range a.typePrimitiveAllocations {
		*v = Ydb.Type_TypeId{}
		typePrimitivePool.Put(v)
	}
	a.typePrimitiveAllocations = a.typePrimitiveAllocations[:0]
	for _, v := range a.typeDecimalAllocations {
		*v = Ydb.Type_DecimalType{}
		typeDecimalPool.Put(v)
	}
	a.typeDecimalAllocations = a.typeDecimalAllocations[:0]
	for _, v := range a.typeListAllocations {
		*v = Ydb.Type_ListType{}
		typeListPool.Put(v)
	}
	a.typeListAllocations = a.typeListAllocations[:0]
	for _, v := range a.typeEmptyListAllocations {
		*v = Ydb.Type_EmptyListType{}
		typeEmptyListPool.Put(v)
	}
	a.typeEmptyListAllocations = a.typeEmptyListAllocations[:0]
	for _, v := range a.typeTupleAllocations {
		*v = Ydb.Type_TupleType{}
		typeTuplePool.Put(v)
	}
	a.typeTupleAllocations = a.typeTupleAllocations[:0]
	for _, v := range a.decimalAllocations {
		*v = Ydb.DecimalType{}
		v.Reset()
		decimalPool.Put(v)
	}
	a.decimalAllocations = a.decimalAllocations[:0]
	for _, v := range a.listAllocations {
		v.Reset()
		listPool.Put(v)
	}
	a.listAllocations = a.listAllocations[:0]
	for _, v := range a.tupleAllocations {
		v.Reset()
		tuplePool.Put(v)
	}
	a.tupleAllocations = a.tupleAllocations[:0]
	for _, v := range a.typeOptionalAllocations {
		*v = Ydb.Type_OptionalType{}
		typeOptionalPool.Put(v)
	}
	a.typeOptionalAllocations = a.typeOptionalAllocations[:0]
	for _, v := range a.typedValueAllocations {
		v.Reset()
		typedValuePool.Put(v)
	}
	a.typedValueAllocations = a.typedValueAllocations[:0]
	for _, v := range a.boolAllocations {
		*v = Ydb.Value_BoolValue{}
		boolPool.Put(v)
	}
	a.boolAllocations = a.boolAllocations[:0]
	for _, v := range a.bytesAllocations {
		*v = Ydb.Value_BytesValue{}
		bytesPool.Put(v)
	}
	a.bytesAllocations = a.bytesAllocations[:0]
	for _, v := range a.textAllocations {
		*v = Ydb.Value_TextValue{}
		textPool.Put(v)
	}
	a.textAllocations = a.textAllocations[:0]
	for _, v := range a.int32Allocations {
		*v = Ydb.Value_Int32Value{}
		int32Pool.Put(v)
	}
	a.int32Allocations = a.int32Allocations[:0]
	for _, v := range a.uint32Allocations {
		*v = Ydb.Value_Uint32Value{}
		uint32Pool.Put(v)
	}
	a.uint32Allocations = a.uint32Allocations[:0]
	for _, v := range a.low128Allocations {
		*v = Ydb.Value_Low_128{}
		low128Pool.Put(v)
	}
	a.low128Allocations = a.low128Allocations[:0]
	for _, v := range a.int64Allocations {
		*v = Ydb.Value_Int64Value{}
		int64Pool.Put(v)
	}
	a.int64Allocations = a.int64Allocations[:0]
	for _, v := range a.uint64Allocations {
		*v = Ydb.Value_Uint64Value{}
		uint64Pool.Put(v)
	}
	a.uint64Allocations = a.uint64Allocations[:0]
	for _, v := range a.floatAllocations {
		*v = Ydb.Value_FloatValue{}
		floatPool.Put(v)
	}
	a.floatAllocations = a.floatAllocations[:0]
	for _, v := range a.doubleAllocations {
		*v = Ydb.Value_DoubleValue{}
		doublePool.Put(v)
	}
	a.doubleAllocations = a.doubleAllocations[:0]
	allocatorsPool.Put(a)
}

func (a *Allocator) Value() (v *Ydb.Value) {
	v = valuePool.Get()
	a.valueAllocations = append(a.valueAllocations, v)
	return v
}

func (a *Allocator) TypedValue() (v *Ydb.TypedValue) {
	v = typedValuePool.Get()
	a.typedValueAllocations = append(a.typedValueAllocations, v)
	return v
}

func (a *Allocator) Type() (v *Ydb.Type) {
	v = typePool.Get()
	a.typeAllocations = append(a.typeAllocations, v)
	return v
}

func (a *Allocator) TypePrimitive() (v *Ydb.Type_TypeId) {
	v = typePrimitivePool.Get()
	a.typePrimitiveAllocations = append(a.typePrimitiveAllocations, v)
	return v
}

func (a *Allocator) Decimal() (v *Ydb.DecimalType) {
	v = decimalPool.Get()
	a.decimalAllocations = append(a.decimalAllocations, v)
	return v
}

func (a *Allocator) List() (v *Ydb.ListType) {
	v = listPool.Get()
	a.listAllocations = append(a.listAllocations, v)
	return v
}

func (a *Allocator) Tuple() (v *Ydb.TupleType) {
	v = tuplePool.Get()
	a.tupleAllocations = append(a.tupleAllocations, v)
	return v
}

func (a *Allocator) TypeDecimal() (v *Ydb.Type_DecimalType) {
	v = typeDecimalPool.Get()
	a.typeDecimalAllocations = append(a.typeDecimalAllocations, v)
	return v
}

func (a *Allocator) TypeList() (v *Ydb.Type_ListType) {
	v = typeListPool.Get()
	a.typeListAllocations = append(a.typeListAllocations, v)
	return v
}

func (a *Allocator) TypeTuple() (v *Ydb.Type_TupleType) {
	v = typeTuplePool.Get()
	a.typeTupleAllocations = append(a.typeTupleAllocations, v)
	return v
}

func (a *Allocator) EmptyTypeList() (v *Ydb.Type_EmptyListType) {
	v = typeEmptyListPool.Get()
	a.typeEmptyListAllocations = append(a.typeEmptyListAllocations, v)
	return v
}

func (a *Allocator) TypeOptional() (v *Ydb.Type_OptionalType) {
	v = typeOptionalPool.Get()
	a.typeOptionalAllocations = append(a.typeOptionalAllocations, v)
	return v
}

func (a *Allocator) BoolValue() (v *Ydb.Value_BoolValue) {
	v = boolPool.Get()
	a.boolAllocations = append(a.boolAllocations, v)
	return v
}

func (a *Allocator) BytesValue() (v *Ydb.Value_BytesValue) {
	v = bytesPool.Get()
	a.bytesAllocations = append(a.bytesAllocations, v)
	return v
}

func (a *Allocator) Int32Value() (v *Ydb.Value_Int32Value) {
	v = int32Pool.Get()
	a.int32Allocations = append(a.int32Allocations, v)
	return v
}

func (a *Allocator) Int64Value() (v *Ydb.Value_Int64Value) {
	v = int64Pool.Get()
	a.int64Allocations = append(a.int64Allocations, v)
	return v
}

func (a *Allocator) Uint32Value() (v *Ydb.Value_Uint32Value) {
	v = uint32Pool.Get()
	a.uint32Allocations = append(a.uint32Allocations, v)
	return v
}

func (a *Allocator) FloatValue() (v *Ydb.Value_FloatValue) {
	v = floatPool.Get()
	a.floatAllocations = append(a.floatAllocations, v)
	return v
}

func (a *Allocator) DoubleValue() (v *Ydb.Value_DoubleValue) {
	v = doublePool.Get()
	a.doubleAllocations = append(a.doubleAllocations, v)
	return v
}

func (a *Allocator) Uint64Value() (v *Ydb.Value_Uint64Value) {
	v = uint64Pool.Get()
	a.uint64Allocations = append(a.uint64Allocations, v)
	return v
}

func (a *Allocator) TextValue() (v *Ydb.Value_TextValue) {
	v = textPool.Get()
	a.textAllocations = append(a.textAllocations, v)
	return v
}

func (a *Allocator) Low128Value() (v *Ydb.Value_Low_128) {
	v = low128Pool.Get()
	a.low128Allocations = append(a.low128Allocations, v)
	return v
}
