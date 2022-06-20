package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type int16Value int16

func (v int16Value) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()

	typePrimitive.TypeId = Ydb.Type_INT16

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v int16Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int32()
	vv.Int32Value = int32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int16Value(v int16) int16Value {
	return int16Value(v)
}
