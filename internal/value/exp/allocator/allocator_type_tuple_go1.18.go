//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typeTupleAllocator struct {
	allocations []*Ydb.Type_TupleType
}

func (a *typeTupleAllocator) TypeTuple() (v *Ydb.Type_TupleType) {
	v = typeTuplePool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *typeTupleAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_TupleType{}
		typeTuplePool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
