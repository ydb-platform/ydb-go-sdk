package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type uint64Value uint64

func (v uint64Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v uint64Value) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (uint64Value) Type() T {
	return TypeUint64
}

func (v uint64Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint64()
	vv.Uint64Value = uint64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint64Value(v uint64) uint64Value {
	return uint64Value(v)
}
