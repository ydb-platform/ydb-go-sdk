package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type int64Value int64

func (v int64Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v int64Value) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (int64Value) Type() T {
	return TypeInt64
}

func (v int64Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int64()
	vv.Int64Value = int64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int64Value(v int64) int64Value {
	return int64Value(v)
}
