package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type uint32Value uint32

func (v uint32Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v uint32Value) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (uint32Value) getType() T {
	return TypeUint32
}

func (uint32Value) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeUint32]
}

func (v uint32Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint32Value(v uint32) uint32Value {
	return uint32Value(v)
}
