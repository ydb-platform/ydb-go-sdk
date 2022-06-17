package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type uint16Value uint16

func (v uint16Value) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typeId := a.TypePrimitive()
	typeId.TypeId = Ydb.Type_UINT16

	t := a.Type()
	t.Type = typeId

	return t
}

func (v uint16Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32Value()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint16Value(v uint16) uint16Value {
	return uint16Value(v)
}
