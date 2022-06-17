package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type floatValue float32

func (v floatValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typeId := a.TypePrimitive()
	typeId.TypeId = Ydb.Type_FLOAT

	t := a.Type()
	t.Type = typeId

	return t
}

func (v floatValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.FloatValue()
	vv.FloatValue = float32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func FloatValue(v float32) floatValue {
	return floatValue(v)
}
