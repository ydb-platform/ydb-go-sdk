//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type nullFlagAllocator struct {
	allocations []*Ydb.Value_NullFlagValue
}

func (a *nullFlagAllocator) NullFlag() (v *Ydb.Value_NullFlagValue) {
	v = nullFlagPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *nullFlagAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_NullFlagValue{}
		nullFlagPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
