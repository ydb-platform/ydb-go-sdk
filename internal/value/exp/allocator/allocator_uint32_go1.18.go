//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type uint32Allocator struct {
	allocations []*Ydb.Value_Uint32Value
}

func (a *uint32Allocator) Uint32() (v *Ydb.Value_Uint32Value) {
	v = uint32Pool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *uint32Allocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_Uint32Value{}
		uint32Pool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
