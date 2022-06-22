package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type stringValue []byte

func (v stringValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v stringValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (stringValue) getType() T {
	return TypeString
}

func (stringValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeString]
}

func (v stringValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Bytes()

	vv.BytesValue = v

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func StringValue(v []byte) stringValue {
	return stringValue(v)
}
