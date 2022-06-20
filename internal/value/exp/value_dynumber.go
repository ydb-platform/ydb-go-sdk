package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type dyNumberValue struct {
	v string
}

func (dyNumberValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()
	typePrimitive.TypeId = Ydb.Type_DYNUMBER

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v *dyNumberValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.TextValue()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DyNumberValue(v string) *dyNumberValue {
	return &dyNumberValue{v: v}
}
