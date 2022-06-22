package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type dateValue uint32

func (v dateValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v dateValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (dateValue) getType() T {
	return TypeDate
}

func (dateValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeDate]
}

func (v dateValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()

	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DateValue(v uint32) dateValue {
	return dateValue(v)
}
