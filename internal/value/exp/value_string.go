package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type stringValue []byte

func (v stringValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()

	typePrimitive.TypeId = Ydb.Type_STRING

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v stringValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Bytes()

	vv.BytesValue = v

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func StringValue(v []byte) stringValue {
	return stringValue(v)
}
