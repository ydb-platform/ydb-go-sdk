package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type int8Value int8

func (v int8Value) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()
	typePrimitive.TypeId = Ydb.Type_INT8

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v int8Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int32()
	vv.Int32Value = int32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int8Value(v int8) int8Value {
	return int8Value(v)
}
