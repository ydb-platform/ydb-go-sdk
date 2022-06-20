package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type floatValue struct {
	v float32
}

func (*floatValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()
	typePrimitive.TypeId = Ydb.Type_FLOAT

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v *floatValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Float()
	if v != nil {
		vv.FloatValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func FloatValue(v float32) *floatValue {
	return &floatValue{v: v}
}
