package value

import (
	"bytes"
	"encoding/binary"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type uuidValue struct {
	v [16]byte
}

func (v *uuidValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *uuidValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (*uuidValue) getType() T {
	return TypeUUID
}

func (*uuidValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeUUID]
}

func (v *uuidValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	var bytes [16]byte
	if v != nil {
		bytes = v.v
	}
	vv := a.Low128()
	vv.Low_128 = binary.BigEndian.Uint64(bytes[8:16])

	vvv := a.Value()
	vvv.High_128 = binary.BigEndian.Uint64(bytes[0:8])
	vvv.Value = vv

	return vvv
}

func UUIDValue(v [16]byte) *uuidValue {
	return &uuidValue{v: v}
}
