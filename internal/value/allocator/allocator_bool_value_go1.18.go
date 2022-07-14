//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type boolAllocator struct {
	allocations []*Ydb.Value_BoolValue
}

func (a *boolAllocator) Bool() (v *Ydb.Value_BoolValue) {
	v = boolPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *boolAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_BoolValue{}
		boolPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
