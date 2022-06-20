package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type utf8Value struct {
	v string
}

func (*utf8Value) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()
	typePrimitive.TypeId = Ydb.Type_UTF8

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v *utf8Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func UTF8Value(v string) *utf8Value {
	return &utf8Value{v: v}
}
