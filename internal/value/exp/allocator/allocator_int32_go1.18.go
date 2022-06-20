//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type int32Allocator struct {
	allocations []*Ydb.Value_Int32Value
}

func (a *int32Allocator) Int32() (v *Ydb.Value_Int32Value) {
	v = int32Pool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *int32Allocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_Int32Value{}
		int32Pool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
