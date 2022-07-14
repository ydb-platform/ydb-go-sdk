//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type uint64Allocator struct {
	allocations []*Ydb.Value_Uint64Value
}

func (a *uint64Allocator) Uint64() (v *Ydb.Value_Uint64Value) {
	v = uint64Pool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *uint64Allocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_Uint64Value{}
		uint64Pool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
