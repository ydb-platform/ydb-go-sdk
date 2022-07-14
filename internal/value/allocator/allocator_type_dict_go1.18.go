//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typeDictAllocator struct {
	allocations []*Ydb.Type_DictType
}

func (a *typeDictAllocator) TypeDict() (v *Ydb.Type_DictType) {
	v = typeDictPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *typeDictAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_DictType{}
		typeDictPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
