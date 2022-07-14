//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type tupleAllocator struct {
	allocations []*Ydb.TupleType
}

func (a *tupleAllocator) Tuple() (v *Ydb.TupleType) {
	v = tuplePool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *tupleAllocator) free() {
	for _, v := range a.allocations {
		elements := v.Elements
		for i := range elements {
			elements[i] = nil
		}
		v.Reset()
		v.Elements = elements[:0]
		tuplePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
