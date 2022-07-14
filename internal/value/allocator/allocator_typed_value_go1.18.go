//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typedValueAllocator struct {
	allocations []*Ydb.TypedValue
}

func (a *typedValueAllocator) TypedValue() (v *Ydb.TypedValue) {
	v = typedValuePool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *typedValueAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		typedValuePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
