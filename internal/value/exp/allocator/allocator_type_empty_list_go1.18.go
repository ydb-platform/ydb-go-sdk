//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typeEmptyListAllocator struct {
	allocations []*Ydb.Type_EmptyListType
}

func (a *typeEmptyListAllocator) TypeEmptyList() (v *Ydb.Type_EmptyListType) {
	v = typeEmptyListPool.Get()
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
