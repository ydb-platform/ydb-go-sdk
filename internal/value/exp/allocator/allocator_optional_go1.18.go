//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type optionalAllocator struct {
	allocations []*Ydb.OptionalType
}

func (a *optionalAllocator) Optional() (v *Ydb.OptionalType) {
	v = optionalPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *optionalAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		optionalPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
