//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type low128Allocator struct {
	allocations []*Ydb.Value_Low_128
}

func (a *low128Allocator) Low128() (v *Ydb.Value_Low_128) {
	v = low128Pool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *low128Allocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_Low_128{}
		low128Pool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
