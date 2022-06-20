//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type doubleAllocator struct {
	allocations []*Ydb.Value_DoubleValue
}

func (a *doubleAllocator) Double() (v *Ydb.Value_DoubleValue) {
	v = doublePool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *doubleAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_DoubleValue{}
		doublePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
