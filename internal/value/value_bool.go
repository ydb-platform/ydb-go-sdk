package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type boolValue bool

func (v boolValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v boolValue) String() string {
	if v {
		return "Bool(true)"
	}
	return "Bool(false)"
}

func (boolValue) Type() T {
	return TypeBool
}

func (v boolValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Bool()

	vv.BoolValue = bool(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func BoolValue(v bool) boolValue {
	return boolValue(v)
}
