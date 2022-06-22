package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type int64Value int64

func (v int64Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v int64Value) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (int64Value) getType() T {
	return TypeInt64
}

func (int64Value) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeInt64]
}

func (v int64Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int64()
	vv.Int64Value = int64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int64Value(v int64) int64Value {
	return int64Value(v)
}
