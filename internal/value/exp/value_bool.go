package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type boolValue bool

func (v boolValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v boolValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (boolValue) getType() T {
	return TypeBool
}

func (boolValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeBool]
}

func (v boolValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Bool()

	vv.BoolValue = bool(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func BoolValue(v bool) boolValue {
	return boolValue(v)
}
