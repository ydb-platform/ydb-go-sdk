//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type variantAllocator struct {
	allocations []*Ydb.VariantType
}

func (a *variantAllocator) Variant() (v *Ydb.VariantType) {
	v = variantPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *variantAllocator) free() {
	for _, v := range a.allocations {
		variantPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
