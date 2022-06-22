//go:build go1.18
// +build go1.18

package allocator

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

type typeVariantAllocator struct {
	allocations []*Ydb.Type_VariantType
}

func (a *typeVariantAllocator) TypeVariant() (v *Ydb.Type_VariantType) {
	v = typeVariantPool.Get()
	a.allocations = append(a.allocations, v)
	return v
}

func (a *typeVariantAllocator) free() {
	for _, v := range a.allocations {
		*v = Ydb.Type_VariantType{}
		typeVariantPool.Put(v)
	}
	a.allocations = a.allocations[:0]
}
