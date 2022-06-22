package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type int16Value int16

func (v int16Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v int16Value) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (int16Value) getType() T {
	return TypeInt16
}

func (int16Value) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeInt16]
}

func (v int16Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int32()
	vv.Int32Value = int32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int16Value(v int16) int16Value {
	return int16Value(v)
}
