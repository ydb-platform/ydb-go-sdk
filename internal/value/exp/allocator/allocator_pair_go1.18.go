//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type pairAllocator struct {
	allocations []*Ydb.ValuePair
}

func (a *pairAllocator) Pair() (v *Ydb.ValuePair) {
	v = pairPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *pairAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.ValuePair{}
		pairPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
