package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type int8Value int8

func (v int8Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v int8Value) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (int8Value) getType() T {
	return TypeInt8
}

func (int8Value) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeInt8]
}

func (v int8Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int32()
	vv.Int32Value = int32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int8Value(v int8) int8Value {
	return int8Value(v)
}
