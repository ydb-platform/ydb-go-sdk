//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type structAllocator struct {
	allocations []*Ydb.StructType
}

func (a *structAllocator) Struct() (v *Ydb.StructType) {
	v = structPool.Get()
	if cap(v.Members) <= 0 {
		v.Members = make([]*Ydb.StructMember, 0, 10)
	}
	a.allocations = append(a.allocations, v)
	return v
}

func (a *structAllocator) free() {
	for _, v := range a.allocations {
		members := v.Members
		for i := range members {
			members[i] = nil
		}
		v.Reset()
		v.Members = members[:0]
		structPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
