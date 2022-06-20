package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type jsonValue struct {
	v string
}

func (*jsonValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()
	typePrimitive.TypeId = Ydb.Type_JSON

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v *jsonValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.TextValue()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func JSONValue(v string) *jsonValue {
	return &jsonValue{v: v}
}
