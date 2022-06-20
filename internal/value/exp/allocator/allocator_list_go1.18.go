//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type listAllocator struct {
	allocations []*Ydb.ListType
}

func (a *listAllocator) List() (v *Ydb.ListType) {
	v = listPool.Get()
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
