//go:build go1.18
// +build go1.18

package allocator

type (
	Allocator struct {
		valueAllocator
		typeAllocator
		typedValueAllocator
		boolAllocator
		typeDecimalAllocator
		typeListAllocator
		typeEmptyListAllocator
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
	}
)

func New() (v *Allocator) {
	return allocatorsPool.Get()
}

func (a *Allocator) Free() {
	a.valueAllocator.free()
	a.typeAllocator.free()
	a.typedValueAllocator.free()
	a.boolAllocator.free()
	a.typeDecimalAllocator.free()
	a.typeListAllocator.free()
	a.typeEmptyListAllocator.free()
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

	allocatorsPool.Put(a)
}
