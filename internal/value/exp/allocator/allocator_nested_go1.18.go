//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type nestedAllocator struct {
	allocations []*Ydb.Value_NestedValue
}

func (a *nestedAllocator) Nested() (v *Ydb.Value_NestedValue) {
	v = nestedPool.Get()
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
