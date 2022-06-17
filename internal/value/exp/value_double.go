package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type doubleValue float64

func (v doubleValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typeId := a.TypePrimitive()
	typeId.TypeId = Ydb.Type_DOUBLE

	t := a.Type()
	t.Type = typeId

	return t
}

func (v doubleValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.DoubleValue()
	vv.DoubleValue = float64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DoubleValue(v float64) doubleValue {
	return doubleValue(v)
}
