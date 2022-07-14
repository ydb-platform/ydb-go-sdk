//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typeOptionalAllocator struct {
	allocations []*Ydb.Type_OptionalType
}

func (a *typeOptionalAllocator) TypeOptional() (v *Ydb.Type_OptionalType) {
	v = typeOptionalPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *typeOptionalAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_OptionalType{}
		typeOptionalPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
