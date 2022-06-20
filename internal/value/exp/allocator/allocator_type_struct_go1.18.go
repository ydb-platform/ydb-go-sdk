//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typeStructAllocator struct {
	allocations []*Ydb.Type_StructType
}

func (a *typeStructAllocator) TypeStruct() (v *Ydb.Type_StructType) {
	v = typeStructPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *typeStructAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_StructType{}
		typeStructPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
