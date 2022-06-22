package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type uint64Value uint64

func (v uint64Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v uint64Value) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (uint64Value) getType() T {
	return TypeUint64
}

func (uint64Value) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeUint64]
}

func (v uint64Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint64()
	vv.Uint64Value = uint64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint64Value(v uint64) uint64Value {
	return uint64Value(v)
}
