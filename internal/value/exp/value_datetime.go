package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type datetimeValue uint32

func (v datetimeValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()
	typePrimitive.TypeId = Ydb.Type_DATETIME

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v datetimeValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DatetimeValue(v uint32) datetimeValue {
	return datetimeValue(v)
}
