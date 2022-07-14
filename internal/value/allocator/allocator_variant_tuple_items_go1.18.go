//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type variantTupleItemsAllocator struct {
	allocations []*Ydb.VariantType_TupleItems
}

func (a *variantTupleItemsAllocator) VariantTupleItems() (v *Ydb.VariantType_TupleItems) {
	v = variantTupleItemsPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *variantTupleItemsAllocator) free() {
	for _, v := range a.allocations {
		variantTupleItemsPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
