//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type textAllocator struct {
	allocations []*Ydb.Value_TextValue
}

func (a *textAllocator) Text() (v *Ydb.Value_TextValue) {
	v = textPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *textAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Value_TextValue{}
		textPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
