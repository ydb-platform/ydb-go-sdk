package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type intervalValue int64

func (v intervalValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()
	typePrimitive.TypeId = Ydb.Type_INTERVAL

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v intervalValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int64()
	vv.Int64Value = int64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

// IntervalValue makes Value from given microseconds value
func IntervalValue(v int64) intervalValue {
	return intervalValue(v)
}
