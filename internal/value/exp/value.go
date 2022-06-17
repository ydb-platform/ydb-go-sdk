package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type V interface {
	toYDBType(a *allocator.Allocator) *Ydb.Type
	toYDBValue(a *allocator.Allocator) *Ydb.Value
}

func typedValue(v V, a *allocator.Allocator) *Ydb.TypedValue {
	tv := a.TypedValue()

	tv.Type = v.toYDBType(a)
	tv.Value = v.toYDBValue(a)

	return tv
}
