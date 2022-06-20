//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type int64Allocator struct {
	allocations []*Ydb.Value_Int64Value
}

func (a *int64Allocator) Int64() (v *Ydb.Value_Int64Value) {
	v = int64Pool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *int64Allocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_Int64Value{}
		int64Pool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
