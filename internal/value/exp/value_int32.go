package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type int32Value int32

func (v int32Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v int32Value) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (int32Value) getType() T {
	return TypeInt32
}

func (int32Value) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeInt32]
}

func (v int32Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int32()
	vv.Int32Value = int32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int32Value(v int32) int32Value {
	return int32Value(v)
}
