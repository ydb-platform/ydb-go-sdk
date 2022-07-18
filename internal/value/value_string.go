package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type stringValue []byte

func (v stringValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v stringValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (stringValue) Type() T {
	return TypeString
}

func (v stringValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Bytes()

	vv.BytesValue = v

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func StringValue(v []byte) stringValue {
	return stringValue(v)
}
