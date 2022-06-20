//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typeAllocator struct {
	allocations []*Ydb.Type
}

func (a *typeAllocator) Type() (v *Ydb.Type) {
	v = typePool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *typeAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		typePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
