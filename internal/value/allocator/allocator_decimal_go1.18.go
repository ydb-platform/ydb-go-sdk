//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type decimalAllocator struct {
	allocations []*Ydb.DecimalType
}

func (a *decimalAllocator) Decimal() (v *Ydb.DecimalType) {
	v = decimalPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *decimalAllocator) free() {
	for _, v := range a.allocations {
		v.Reset()
		decimalPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
