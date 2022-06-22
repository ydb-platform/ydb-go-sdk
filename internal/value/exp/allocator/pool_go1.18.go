//go:build go1.18
// +build go1.18

package allocator

import (
	"sync"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

var (
	allocatorsPool         pool[Allocator]
	valuePool              pool[Ydb.Value]
	typePool               pool[Ydb.Type]
	typeDecimalPool        pool[Ydb.Type_DecimalType]
	typeListPool           pool[Ydb.Type_ListType]
	typeEmptyListPool      pool[Ydb.Type_EmptyListType]
	typeTuplePool          pool[Ydb.Type_TupleType]
	typeStructPool         pool[Ydb.Type_StructType]
	typeDictPool           pool[Ydb.Type_DictType]
	typeVariantPool        pool[Ydb.Type_VariantType]
	decimalPool            pool[Ydb.DecimalType]
	listPool               pool[Ydb.ListType]
	tuplePool              pool[Ydb.TupleType]
	structPool             pool[Ydb.StructType]
	dictPool               pool[Ydb.DictType]
	variantPool            pool[Ydb.VariantType]
	variantTupleItemsPool  pool[Ydb.VariantType_TupleItems]
	variantStructItemsPool pool[Ydb.VariantType_StructItems]
	structMemberPool       pool[Ydb.StructMember]
	typeOptionalPool       pool[Ydb.Type_OptionalType]
	optionalPool           pool[Ydb.OptionalType]
	typedValuePool         pool[Ydb.TypedValue]
	boolPool               pool[Ydb.Value_BoolValue]
	bytesPool              pool[Ydb.Value_BytesValue]
	textPool               pool[Ydb.Value_TextValue]
	int32Pool              pool[Ydb.Value_Int32Value]
	uint32Pool             pool[Ydb.Value_Uint32Value]
	low128Pool             pool[Ydb.Value_Low_128]
	int64Pool              pool[Ydb.Value_Int64Value]
	uint64Pool             pool[Ydb.Value_Uint64Value]
	floatPool              pool[Ydb.Value_FloatValue]
	doublePool             pool[Ydb.Value_DoubleValue]
	nestedPool             pool[Ydb.Value_NestedValue]
	nullFlagPool           pool[Ydb.Value_NullFlagValue]
	pairPool               pool[Ydb.ValuePair]
)

type pool[T any] sync.Pool

func (p *pool[T]) Get() *T {
	v := (*sync.Pool)(p).Get()
	if v == nil {
		var zero T
		v = &zero
	}
	return v.(*T)
}

func (p *pool[T]) Put(t *T) {
	(*sync.Pool)(p).Put(t)
}
