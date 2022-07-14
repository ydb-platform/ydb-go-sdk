//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type structMemberAllocator struct {
	allocations []*Ydb.StructMember
}

func (a *structMemberAllocator) StructMember() (v *Ydb.StructMember) {
	v = structMemberPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *structMemberAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		structMemberPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
