//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typePrimitiveAllocator struct {
	allocations []*Ydb.Type_TypeId
}

func (a *typePrimitiveAllocator) TypePrimitive() (v *Ydb.Type_TypeId) {
	v = typePrimitivePool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *typePrimitiveAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_TypeId{}
		typePrimitivePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
