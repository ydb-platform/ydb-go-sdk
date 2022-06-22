package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type uint8Value uint8

func (v uint8Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v uint8Value) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (uint8Value) getType() T {
	return TypeUint8
}

func (uint8Value) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeUint8]
}

func (v uint8Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint8Value(v uint8) uint8Value {
	return uint8Value(v)
}
