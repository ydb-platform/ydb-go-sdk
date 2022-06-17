package value

import (
	"encoding/binary"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type uuidValue [16]byte

func (v uuidValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()
	typePrimitive.TypeId = Ydb.Type_UUID

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v uuidValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Low128Value()
	vv.Low_128 = binary.BigEndian.Uint64(v[8:16])

	vvv := a.Value()
	vvv.High_128 = binary.BigEndian.Uint64(v[0:8])
	vvv.Value = vv

	return vvv
}

func UUIDValue(v [16]byte) uuidValue {
	return uuidValue(v)
}
