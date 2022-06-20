//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type bytesAllocator struct {
	allocations []*Ydb.Value_BytesValue
}

func (a *bytesAllocator) Bytes() (v *Ydb.Value_BytesValue) {
	v = bytesPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *bytesAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_BytesValue{}
		bytesPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
