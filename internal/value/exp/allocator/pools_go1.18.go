//go:build go1.18
// +build go1.18

package allocator

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/pool"
)

var (
	allocatorsPool         pool.Pool[Allocator]
	valuePool              pool.Pool[Ydb.Value]
	typePool               pool.Pool[Ydb.Type]
	typeDecimalPool        pool.Pool[Ydb.Type_DecimalType]
	typeListPool           pool.Pool[Ydb.Type_ListType]
	typeEmptyListPool      pool.Pool[Ydb.Type_EmptyListType]
	typeTuplePool          pool.Pool[Ydb.Type_TupleType]
	typeStructPool         pool.Pool[Ydb.Type_StructType]
	typeDictPool           pool.Pool[Ydb.Type_DictType]
	typeVariantPool        pool.Pool[Ydb.Type_VariantType]
	decimalPool            pool.Pool[Ydb.DecimalType]
	listPool               pool.Pool[Ydb.ListType]
	tuplePool              pool.Pool[Ydb.TupleType]
	structPool             pool.Pool[Ydb.StructType]
	dictPool               pool.Pool[Ydb.DictType]
	variantPool            pool.Pool[Ydb.VariantType]
	variantTupleItemsPool  pool.Pool[Ydb.VariantType_TupleItems]
	variantStructItemsPool pool.Pool[Ydb.VariantType_StructItems]
	structMemberPool       pool.Pool[Ydb.StructMember]
	typeOptionalPool       pool.Pool[Ydb.Type_OptionalType]
	optionalPool           pool.Pool[Ydb.OptionalType]
	typedValuePool         pool.Pool[Ydb.TypedValue]
	boolPool               pool.Pool[Ydb.Value_BoolValue]
	bytesPool              pool.Pool[Ydb.Value_BytesValue]
	textPool               pool.Pool[Ydb.Value_TextValue]
	int32Pool              pool.Pool[Ydb.Value_Int32Value]
	uint32Pool             pool.Pool[Ydb.Value_Uint32Value]
	low128Pool             pool.Pool[Ydb.Value_Low_128]
	int64Pool              pool.Pool[Ydb.Value_Int64Value]
	uint64Pool             pool.Pool[Ydb.Value_Uint64Value]
	floatPool              pool.Pool[Ydb.Value_FloatValue]
	doublePool             pool.Pool[Ydb.Value_DoubleValue]
	nestedPool             pool.Pool[Ydb.Value_NestedValue]
	nullFlagPool           pool.Pool[Ydb.Value_NullFlagValue]
	pairPool               pool.Pool[Ydb.ValuePair]
)
