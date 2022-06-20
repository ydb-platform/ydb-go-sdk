package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type tzTimestampValue struct {
	v string
}

func (*tzTimestampValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()
	typePrimitive.TypeId = Ydb.Type_TZ_TIMESTAMP

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v *tzTimestampValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.TextValue()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TzTimestampValue(v string) *tzTimestampValue {
	return &tzTimestampValue{v: v}
}
