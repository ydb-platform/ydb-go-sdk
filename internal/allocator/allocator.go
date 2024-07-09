package allocator

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type (
	Allocator struct {
		valueAllocator
		typeAllocator
		typedValueAllocator
		boolAllocator
		typeDecimalAllocator
		typeListAllocator
		typeEmptyListAllocator
		typeEmptyDictAllocator
		typeTupleAllocator
		typeStructAllocator
		typeDictAllocator
		decimalAllocator
		listAllocator
		tupleAllocator
		structAllocator
		dictAllocator
		structMemberAllocator
		typeOptionalAllocator
		optionalAllocator
		bytesAllocator
		textAllocator
		uint32Allocator
		int32Allocator
		low128Allocator
		uint64Allocator
		int64Allocator
		floatAllocator
		doubleAllocator
		nestedAllocator
		pairAllocator
		nullFlagAllocator
		variantAllocator
		typeVariantAllocator
		variantStructItemsAllocator
		variantTupleItemsAllocator
		tableExecuteQueryResultAllocator
		tableExecuteQueryRequestAllocator
		tableQueryCachePolicyAllocator
		tableQueryAllocator
		tableQueryYqlTextAllocator
		tableQueryIDAllocator
		queryExecuteQueryRequestAllocator
		queryExecuteQueryRequestQueryContentAllocator
		queryExecuteQueryResponsePartAllocator
		queryQueryContentAllocator
		queryTransactionControlAllocator
		queryTransactionControlBeginTxAllocator
		queryTransactionControlTxIDAllocator
		queryTransactionSettingsAllocator
		queryTransactionSettingsSerializableReadWriteAllocator
	}
)

func New() (v *Allocator) {
	return allocatorPool.GetOrNew()
}

//nolint:funlen
func (a *Allocator) Free() {
	a.valueAllocator.free()
	a.typeAllocator.free()
	a.typedValueAllocator.free()
	a.boolAllocator.free()
	a.typeDecimalAllocator.free()
	a.typeListAllocator.free()
	a.typeEmptyListAllocator.free()
	a.typeEmptyDictAllocator.free()
	a.typeTupleAllocator.free()
	a.typeStructAllocator.free()
	a.typeDictAllocator.free()
	a.decimalAllocator.free()
	a.listAllocator.free()
	a.tupleAllocator.free()
	a.structAllocator.free()
	a.dictAllocator.free()
	a.structMemberAllocator.free()
	a.typeOptionalAllocator.free()
	a.optionalAllocator.free()
	a.bytesAllocator.free()
	a.textAllocator.free()
	a.uint32Allocator.free()
	a.int32Allocator.free()
	a.low128Allocator.free()
	a.uint64Allocator.free()
	a.int64Allocator.free()
	a.floatAllocator.free()
	a.doubleAllocator.free()
	a.nestedAllocator.free()
	a.pairAllocator.free()
	a.nullFlagAllocator.free()
	a.variantAllocator.free()
	a.typeVariantAllocator.free()
	a.variantStructItemsAllocator.free()
	a.variantTupleItemsAllocator.free()
	a.tableExecuteQueryRequestAllocator.free()
	a.tableExecuteQueryResultAllocator.free()
	a.tableQueryCachePolicyAllocator.free()
	a.tableQueryAllocator.free()
	a.tableQueryYqlTextAllocator.free()
	a.tableQueryIDAllocator.free()
	a.queryExecuteQueryRequestAllocator.free()
	a.queryExecuteQueryRequestQueryContentAllocator.free()
	a.queryExecuteQueryResponsePartAllocator.free()
	a.queryQueryContentAllocator.free()
	a.queryTransactionControlAllocator.free()
	a.queryTransactionControlBeginTxAllocator.free()
	a.queryTransactionControlTxIDAllocator.free()
	a.queryTransactionSettingsAllocator.free()
	a.queryTransactionSettingsSerializableReadWriteAllocator.free()

	allocatorPool.Put(a)
}

type boolAllocator struct {
	allocations []*Ydb.Value_BoolValue
}

func (a *boolAllocator) Bool() (v *Ydb.Value_BoolValue) {
	v = boolPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *boolAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_BoolValue{}
		boolPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type bytesAllocator struct {
	allocations []*Ydb.Value_BytesValue
}

func (a *bytesAllocator) Bytes() (v *Ydb.Value_BytesValue) {
	v = bytesPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *bytesAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_BytesValue{}
		bytesPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type decimalAllocator struct {
	allocations []*Ydb.DecimalType
}

func (a *decimalAllocator) Decimal() (v *Ydb.DecimalType) {
	v = decimalPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *decimalAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		decimalPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type dictAllocator struct {
	allocations []*Ydb.DictType
}

func (a *dictAllocator) Dict() (v *Ydb.DictType) {
	v = dictPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *dictAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		dictPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type doubleAllocator struct {
	allocations []*Ydb.Value_DoubleValue
}

func (a *doubleAllocator) Double() (v *Ydb.Value_DoubleValue) {
	v = doublePool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *doubleAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_DoubleValue{}
		doublePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type floatAllocator struct {
	allocations []*Ydb.Value_FloatValue
}

func (a *floatAllocator) Float() (v *Ydb.Value_FloatValue) {
	v = floatPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *floatAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_FloatValue{}
		floatPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type int32Allocator struct {
	allocations []*Ydb.Value_Int32Value
}

func (a *int32Allocator) Int32() (v *Ydb.Value_Int32Value) {
	v = int32Pool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *int32Allocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_Int32Value{}
		int32Pool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type int64Allocator struct {
	allocations []*Ydb.Value_Int64Value
}

func (a *int64Allocator) Int64() (v *Ydb.Value_Int64Value) {
	v = int64Pool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *int64Allocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_Int64Value{}
		int64Pool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type listAllocator struct {
	allocations []*Ydb.ListType
}

func (a *listAllocator) List() (v *Ydb.ListType) {
	v = listPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *listAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		listPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type low128Allocator struct {
	allocations []*Ydb.Value_Low_128
}

func (a *low128Allocator) Low128() (v *Ydb.Value_Low_128) {
	v = low128Pool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *low128Allocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_Low_128{}
		low128Pool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type nestedAllocator struct {
	allocations []*Ydb.Value_NestedValue
}

func (a *nestedAllocator) Nested() (v *Ydb.Value_NestedValue) {
	v = nestedPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *nestedAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_NestedValue{}
		nestedPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type nullFlagAllocator struct {
	allocations []*Ydb.Value_NullFlagValue
}

func (a *nullFlagAllocator) NullFlag() (v *Ydb.Value_NullFlagValue) {
	v = nullFlagPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *nullFlagAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_NullFlagValue{}
		nullFlagPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type optionalAllocator struct {
	allocations []*Ydb.OptionalType
}

func (a *optionalAllocator) Optional() (v *Ydb.OptionalType) {
	v = optionalPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *optionalAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		optionalPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type pairAllocator struct {
	allocations []*Ydb.ValuePair
}

func (a *pairAllocator) Pair() (v *Ydb.ValuePair) {
	v = pairPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *pairAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.ValuePair{}
		pairPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type structAllocator struct {
	allocations []*Ydb.StructType
}

func (a *structAllocator) Struct() (v *Ydb.StructType) {
	v = structPool.GetOrNew()
	if cap(v.GetMembers()) <= 0 {
		v.Members = make([]*Ydb.StructMember, 0, 10) //nolint:gomnd
	}
	a.allocations = append(a.allocations, v)

	return v
}

func (a *structAllocator) free() {
	for _, v := range a.allocations {
		members := v.GetMembers()
		for i := range members {
			members[i] = nil
		}
		v.Reset()
		v.Members = members[:0]
		structPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type structMemberAllocator struct {
	allocations []*Ydb.StructMember
}

func (a *structMemberAllocator) StructMember() (v *Ydb.StructMember) {
	v = structMemberPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *structMemberAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		structMemberPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type textAllocator struct {
	allocations []*Ydb.Value_TextValue
}

func (a *textAllocator) Text() (v *Ydb.Value_TextValue) {
	v = textPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *textAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_TextValue{}
		textPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type tupleAllocator struct {
	allocations []*Ydb.TupleType
}

func (a *tupleAllocator) Tuple() (v *Ydb.TupleType) {
	v = tuplePool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *tupleAllocator) free() {
	for _, v := range a.allocations {
		elements := v.GetElements()
		for i := range elements {
			elements[i] = nil
		}
		v.Reset()
		v.Elements = elements[:0]
		tuplePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typeDecimalAllocator struct {
	allocations []*Ydb.Type_DecimalType
}

func (a *typeDecimalAllocator) TypeDecimal() (v *Ydb.Type_DecimalType) {
	v = typeDecimalPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typeDecimalAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_DecimalType{}
		typeDecimalPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typeDictAllocator struct {
	allocations []*Ydb.Type_DictType
}

func (a *typeDictAllocator) TypeDict() (v *Ydb.Type_DictType) {
	v = typeDictPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typeDictAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_DictType{}
		typeDictPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typeEmptyListAllocator struct {
	allocations []*Ydb.Type_EmptyListType
}

func (a *typeEmptyListAllocator) TypeEmptyList() (v *Ydb.Type_EmptyListType) {
	v = typeEmptyListPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typeEmptyListAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_EmptyListType{}
		typeEmptyListPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typeEmptyDictAllocator struct {
	allocations []*Ydb.Type_EmptyDictType
}

func (a *typeEmptyDictAllocator) TypeEmptyDict() (v *Ydb.Type_EmptyDictType) {
	v = typeEmptyDictPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typeEmptyDictAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_EmptyDictType{}
		typeEmptyDictPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typeAllocator struct {
	allocations []*Ydb.Type
}

func (a *typeAllocator) Type() (v *Ydb.Type) {
	v = typePool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typeAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		typePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typeListAllocator struct {
	allocations []*Ydb.Type_ListType
}

func (a *typeListAllocator) TypeList() (v *Ydb.Type_ListType) {
	v = typeListPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typeListAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_ListType{}
		typeListPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typeOptionalAllocator struct {
	allocations []*Ydb.Type_OptionalType
}

func (a *typeOptionalAllocator) TypeOptional() (v *Ydb.Type_OptionalType) {
	v = typeOptionalPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typeOptionalAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_OptionalType{}
		typeOptionalPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typeStructAllocator struct {
	allocations []*Ydb.Type_StructType
}

func (a *typeStructAllocator) TypeStruct() (v *Ydb.Type_StructType) {
	v = typeStructPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typeStructAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_StructType{}
		typeStructPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typeTupleAllocator struct {
	allocations []*Ydb.Type_TupleType
}

func (a *typeTupleAllocator) TypeTuple() (v *Ydb.Type_TupleType) {
	v = typeTuplePool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typeTupleAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_TupleType{}
		typeTuplePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typeVariantAllocator struct {
	allocations []*Ydb.Type_VariantType
}

func (a *typeVariantAllocator) TypeVariant() (v *Ydb.Type_VariantType) {
	v = typeVariantPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typeVariantAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_VariantType{}
		typeVariantPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type typedValueAllocator struct {
	allocations []*Ydb.TypedValue
}

func (a *typedValueAllocator) TypedValue() (v *Ydb.TypedValue) {
	v = typedValuePool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *typedValueAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		typedValuePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type uint32Allocator struct {
	allocations []*Ydb.Value_Uint32Value
}

func (a *uint32Allocator) Uint32() (v *Ydb.Value_Uint32Value) {
	v = uint32Pool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *uint32Allocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_Uint32Value{}
		uint32Pool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type uint64Allocator struct {
	allocations []*Ydb.Value_Uint64Value
}

func (a *uint64Allocator) Uint64() (v *Ydb.Value_Uint64Value) {
	v = uint64Pool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *uint64Allocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_Uint64Value{}
		uint64Pool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type valueAllocator struct {
	allocations []*Ydb.Value
}

func (a *valueAllocator) Value() (v *Ydb.Value) {
	v = valuePool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *valueAllocator) free() {
	for _, v := range a.allocations {
		items := v.GetItems()
		pairs := v.GetPairs()
		for i := range items {
			items[i] = nil
		}
		for i := range pairs {
			pairs[i] = nil
		}
		v.Reset()
		v.Items = items[:0]
		v.Pairs = pairs[:0]
		valuePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type variantAllocator struct {
	allocations []*Ydb.VariantType
}

func (a *variantAllocator) Variant() (v *Ydb.VariantType) {
	v = variantPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *variantAllocator) free() {
	for _, v := range a.allocations {
		variantPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type variantStructItemsAllocator struct {
	allocations []*Ydb.VariantType_StructItems
}

func (a *variantStructItemsAllocator) VariantStructItems() (v *Ydb.VariantType_StructItems) {
	v = variantStructItemsPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *variantStructItemsAllocator) free() {
	for _, v := range a.allocations {
		variantStructItemsPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type variantTupleItemsAllocator struct {
	allocations []*Ydb.VariantType_TupleItems
}

func (a *variantTupleItemsAllocator) VariantTupleItems() (v *Ydb.VariantType_TupleItems) {
	v = variantTupleItemsPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *variantTupleItemsAllocator) free() {
	for _, v := range a.allocations {
		variantTupleItemsPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type tableExecuteQueryResultAllocator struct {
	allocations []*Ydb_Table.ExecuteQueryResult
}

func (a *tableExecuteQueryResultAllocator) TableExecuteQueryResult() (v *Ydb_Table.ExecuteQueryResult) {
	v = tableExecuteQueryResultPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *tableExecuteQueryResultAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		tableExecuteQueryResultPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type tableExecuteQueryRequestAllocator struct {
	allocations []*Ydb_Table.ExecuteDataQueryRequest
}

func (a *tableExecuteQueryRequestAllocator) TableExecuteDataQueryRequest() (v *Ydb_Table.ExecuteDataQueryRequest) {
	v = tableExecuteDataQueryRequestPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *tableExecuteQueryRequestAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		tableExecuteDataQueryRequestPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type tableQueryCachePolicyAllocator struct {
	allocations []*Ydb_Table.QueryCachePolicy
}

func (a *tableQueryCachePolicyAllocator) TableQueryCachePolicy() (v *Ydb_Table.QueryCachePolicy) {
	v = tableQueryCachePolicyPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *tableQueryCachePolicyAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		tableQueryCachePolicyPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type tableQueryAllocator struct {
	allocations []*Ydb_Table.Query
}

func (a *tableQueryAllocator) TableQuery() (v *Ydb_Table.Query) {
	v = tableQueryPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *tableQueryAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		tableQueryPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type tableQueryYqlTextAllocator struct {
	allocations []*Ydb_Table.Query_YqlText
}

func (a *tableQueryYqlTextAllocator) TableQueryYqlText(s string) (v *Ydb_Table.Query_YqlText) {
	v = tableQueryYqlTextPool.GetOrNew()
	v.YqlText = s
	a.allocations = append(a.allocations, v)

	return v
}

func (a *tableQueryYqlTextAllocator) free() {
	for _, v := range a.allocations {
		tableQueryYqlTextPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type tableQueryIDAllocator struct {
	allocations []*Ydb_Table.Query_Id
}

func (a *tableQueryIDAllocator) TableQueryID(id string) (v *Ydb_Table.Query_Id) {
	v = tableQueryIDPool.GetOrNew()
	v.Id = id
	a.allocations = append(a.allocations, v)

	return v
}

func (a *tableQueryIDAllocator) free() {
	for _, v := range a.allocations {
		tableQueryIDPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type queryExecuteQueryRequestAllocator struct {
	allocations []*Ydb_Query.ExecuteQueryRequest
}

func (a *queryExecuteQueryRequestAllocator) QueryExecuteQueryRequest() (
	v *Ydb_Query.ExecuteQueryRequest,
) {
	v = queryExecuteQueryRequestPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *queryExecuteQueryRequestAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		queryExecuteQueryRequestPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type queryExecuteQueryResponsePartAllocator struct {
	allocations []*Ydb_Query.ExecuteQueryResponsePart
}

func (a *queryExecuteQueryResponsePartAllocator) QueryExecuteQueryResponsePart() (
	v *Ydb_Query.ExecuteQueryResponsePart,
) {
	v = queryExecuteQueryResponsePartPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *queryExecuteQueryResponsePartAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		queryExecuteQueryResponsePartPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type queryExecuteQueryRequestQueryContentAllocator struct {
	allocations []*Ydb_Query.ExecuteQueryRequest_QueryContent
}

func (a *queryExecuteQueryRequestQueryContentAllocator) QueryExecuteQueryRequestQueryContent() (
	v *Ydb_Query.ExecuteQueryRequest_QueryContent,
) {
	v = queryExecuteQueryRequestQueryContentPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *queryExecuteQueryRequestQueryContentAllocator) free() {
	for _, v := range a.allocations {
		queryExecuteQueryRequestQueryContentPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type queryTransactionControlAllocator struct {
	allocations []*Ydb_Query.TransactionControl
}

func (a *queryTransactionControlAllocator) QueryTransactionControl() (v *Ydb_Query.TransactionControl) {
	v = queryTransactionControlPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *queryTransactionControlAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		queryTransactionControlPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type queryTransactionControlBeginTxAllocator struct {
	allocations []*Ydb_Query.TransactionControl_BeginTx
}

func (a *queryTransactionControlBeginTxAllocator) QueryTransactionControlBeginTx() (
	v *Ydb_Query.TransactionControl_BeginTx,
) {
	v = queryTransactionControlBeginTxPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *queryTransactionControlBeginTxAllocator) free() {
	for _, v := range a.allocations {
		queryTransactionControlBeginTxPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type queryTransactionControlTxIDAllocator struct {
	allocations []*Ydb_Query.TransactionControl_TxId
}

func (a *queryTransactionControlTxIDAllocator) QueryTransactionControlTxID() (v *Ydb_Query.TransactionControl_TxId) {
	v = queryTransactionControlTxIDPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *queryTransactionControlTxIDAllocator) free() {
	for _, v := range a.allocations {
		queryTransactionControlTxIDPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type queryTransactionSettingsAllocator struct {
	allocations []*Ydb_Query.TransactionSettings
}

func (a *queryTransactionSettingsAllocator) QueryTransactionSettings() (v *Ydb_Query.TransactionSettings) {
	v = queryTransactionSettingsPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *queryTransactionSettingsAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		queryTransactionSettingsPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type queryTransactionSettingsSerializableReadWriteAllocator struct {
	allocations []*Ydb_Query.TransactionSettings_SerializableReadWrite
}

func (a *queryTransactionSettingsSerializableReadWriteAllocator) QueryTransactionSettingsSerializableReadWrite() (
	v *Ydb_Query.TransactionSettings_SerializableReadWrite,
) {
	v = queryTransactionSettingsSerializableReadWritePool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *queryTransactionSettingsSerializableReadWriteAllocator) free() {
	for _, v := range a.allocations {
		queryTransactionSettingsSerializableReadWritePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

type queryQueryContentAllocator struct {
	allocations []*Ydb_Query.QueryContent
}

func (a *queryQueryContentAllocator) QueryQueryContent() (v *Ydb_Query.QueryContent) {
	v = queryQueryContentPool.GetOrNew()
	a.allocations = append(a.allocations, v)

	return v
}

func (a *queryQueryContentAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		queryQueryContentPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}

var (
	allocatorPool                                     xsync.Pool[Allocator]
	valuePool                                         xsync.Pool[Ydb.Value]
	typePool                                          xsync.Pool[Ydb.Type]
	typeDecimalPool                                   xsync.Pool[Ydb.Type_DecimalType]
	typeListPool                                      xsync.Pool[Ydb.Type_ListType]
	typeEmptyListPool                                 xsync.Pool[Ydb.Type_EmptyListType]
	typeEmptyDictPool                                 xsync.Pool[Ydb.Type_EmptyDictType]
	typeTuplePool                                     xsync.Pool[Ydb.Type_TupleType]
	typeStructPool                                    xsync.Pool[Ydb.Type_StructType]
	typeDictPool                                      xsync.Pool[Ydb.Type_DictType]
	typeVariantPool                                   xsync.Pool[Ydb.Type_VariantType]
	decimalPool                                       xsync.Pool[Ydb.DecimalType]
	listPool                                          xsync.Pool[Ydb.ListType]
	tuplePool                                         xsync.Pool[Ydb.TupleType]
	structPool                                        xsync.Pool[Ydb.StructType]
	dictPool                                          xsync.Pool[Ydb.DictType]
	variantPool                                       xsync.Pool[Ydb.VariantType]
	variantTupleItemsPool                             xsync.Pool[Ydb.VariantType_TupleItems]
	variantStructItemsPool                            xsync.Pool[Ydb.VariantType_StructItems]
	structMemberPool                                  xsync.Pool[Ydb.StructMember]
	typeOptionalPool                                  xsync.Pool[Ydb.Type_OptionalType]
	optionalPool                                      xsync.Pool[Ydb.OptionalType]
	typedValuePool                                    xsync.Pool[Ydb.TypedValue]
	boolPool                                          xsync.Pool[Ydb.Value_BoolValue]
	bytesPool                                         xsync.Pool[Ydb.Value_BytesValue]
	textPool                                          xsync.Pool[Ydb.Value_TextValue]
	int32Pool                                         xsync.Pool[Ydb.Value_Int32Value]
	uint32Pool                                        xsync.Pool[Ydb.Value_Uint32Value]
	low128Pool                                        xsync.Pool[Ydb.Value_Low_128]
	int64Pool                                         xsync.Pool[Ydb.Value_Int64Value]
	uint64Pool                                        xsync.Pool[Ydb.Value_Uint64Value]
	floatPool                                         xsync.Pool[Ydb.Value_FloatValue]
	doublePool                                        xsync.Pool[Ydb.Value_DoubleValue]
	nestedPool                                        xsync.Pool[Ydb.Value_NestedValue]
	nullFlagPool                                      xsync.Pool[Ydb.Value_NullFlagValue]
	pairPool                                          xsync.Pool[Ydb.ValuePair]
	tableExecuteQueryResultPool                       xsync.Pool[Ydb_Table.ExecuteQueryResult]
	tableExecuteDataQueryRequestPool                  xsync.Pool[Ydb_Table.ExecuteDataQueryRequest]
	tableQueryCachePolicyPool                         xsync.Pool[Ydb_Table.QueryCachePolicy]
	tableQueryPool                                    xsync.Pool[Ydb_Table.Query]
	tableQueryYqlTextPool                             xsync.Pool[Ydb_Table.Query_YqlText]
	tableQueryIDPool                                  xsync.Pool[Ydb_Table.Query_Id]
	queryExecuteQueryRequestPool                      xsync.Pool[Ydb_Query.ExecuteQueryRequest]
	queryExecuteQueryRequestQueryContentPool          xsync.Pool[Ydb_Query.ExecuteQueryRequest_QueryContent]
	queryExecuteQueryResponsePartPool                 xsync.Pool[Ydb_Query.ExecuteQueryResponsePart]
	queryQueryContentPool                             xsync.Pool[Ydb_Query.QueryContent]
	queryTransactionControlPool                       xsync.Pool[Ydb_Query.TransactionControl]
	queryTransactionControlBeginTxPool                xsync.Pool[Ydb_Query.TransactionControl_BeginTx]
	queryTransactionControlTxIDPool                   xsync.Pool[Ydb_Query.TransactionControl_TxId]
	queryTransactionSettingsPool                      xsync.Pool[Ydb_Query.TransactionSettings]
	queryTransactionSettingsSerializableReadWritePool xsync.Pool[Ydb_Query.TransactionSettings_SerializableReadWrite]
)
