package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type timestampValue uint64

func (v timestampValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typeId := a.TypePrimitive()
	typeId.TypeId = Ydb.Type_TIMESTAMP

	t := a.Type()
	t.Type = typeId

	return t
}

func (v timestampValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint64Value()
	vv.Uint64Value = uint64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TimestampValue(v uint64) timestampValue {
	return timestampValue(v)
}
