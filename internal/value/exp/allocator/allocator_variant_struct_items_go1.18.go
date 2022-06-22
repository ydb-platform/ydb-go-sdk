//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type variantStructItemsAllocator struct {
	allocations []*Ydb.VariantType_StructItems
}

func (a *variantStructItemsAllocator) VariantStructItems() (v *Ydb.VariantType_StructItems) {
	v = variantStructItemsPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *variantStructItemsAllocator) free() {
	for _, v := range a.allocations {
		variantStructItemsPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
