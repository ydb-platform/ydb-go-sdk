package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type dateValue uint32

func (v dateValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typeId := a.TypePrimitive()

	typeId.TypeId = Ydb.Type_DATE

	t := a.Type()
	t.Type = typeId

	return t
}

func (v dateValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32Value()

	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DateValue(v uint32) dateValue {
	return dateValue(v)
}
