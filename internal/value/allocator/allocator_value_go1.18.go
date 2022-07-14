//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type valueAllocator struct {
	allocations []*Ydb.Value
}

func (a *valueAllocator) Value() (v *Ydb.Value) {
	v = valuePool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *valueAllocator) free() {
	for _, v := range a.allocations {
		items := v.Items
		pairs := v.Pairs
		for i := range items {
			items[i] = nil
		}
		for i := range pairs {
			pairs[i] = nil
		}
		v.Reset()
		v.Items = items[:0]
		v.Pairs = pairs[:0]
		valuePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
