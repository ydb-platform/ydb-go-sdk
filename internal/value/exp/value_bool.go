package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type boolValue bool

func (v boolValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()

	typePrimitive.TypeId = Ydb.Type_BOOL

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v boolValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Bool()

	vv.BoolValue = bool(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func BoolValue(v bool) boolValue {
	return boolValue(v)
}
