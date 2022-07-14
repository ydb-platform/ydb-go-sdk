//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type dictAllocator struct {
	allocations []*Ydb.DictType
}

func (a *dictAllocator) Dict() (v *Ydb.DictType) {
	v = dictPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *dictAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		dictPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
