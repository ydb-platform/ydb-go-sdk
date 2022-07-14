//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typeListAllocator struct {
	allocations []*Ydb.Type_ListType
}

func (a *typeListAllocator) TypeList() (v *Ydb.Type_ListType) {
	v = typeListPool.Get()
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
