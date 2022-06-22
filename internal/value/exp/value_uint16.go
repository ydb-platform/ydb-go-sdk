package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type uint16Value uint16

func (v uint16Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v uint16Value) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (uint16Value) getType() T {
	return TypeUint16
}

func (v uint16Value) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeUint16]
}

func (v uint16Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint16Value(v uint16) uint16Value {
	return uint16Value(v)
}
