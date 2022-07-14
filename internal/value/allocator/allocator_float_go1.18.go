//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type floatAllocator struct {
	allocations []*Ydb.Value_FloatValue
}

func (a *floatAllocator) Float() (v *Ydb.Value_FloatValue) {
	v = floatPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *floatAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_FloatValue{}
		floatPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
