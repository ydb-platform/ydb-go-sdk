//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typeDecimalAllocator struct {
	allocations []*Ydb.Type_DecimalType
}

func (a *typeDecimalAllocator) TypeDecimal() (v *Ydb.Type_DecimalType) {
	v = typeDecimalPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *typeDecimalAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_DecimalType{}
		typeDecimalPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
