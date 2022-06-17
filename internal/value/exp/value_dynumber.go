package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type dyNumberValue string

func (v dyNumberValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typeId := a.TypePrimitive()
	typeId.TypeId = Ydb.Type_DYNUMBER

	t := a.Type()
	t.Type = typeId

	return t
}

func (v dyNumberValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.TextValue()
	vv.TextValue = string(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DyNumberValue(v string) dyNumberValue {
	return dyNumberValue(v)
}
