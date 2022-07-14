package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type uint16Value uint16

func (v uint16Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v uint16Value) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (uint16Value) Type() T {
	return TypeUint16
}

func (v uint16Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint16Value(v uint16) uint16Value {
	return uint16Value(v)
}
