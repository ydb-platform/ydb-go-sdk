package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type optionalValue struct {
	t T
	v V
}

func (v *optionalValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *optionalValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *optionalValue) Type() T {
	return v.t
}

func (v *optionalValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	if _, opt := v.v.(*optionalValue); opt {
		vv := a.Nested()
		vv.NestedValue = v.v.toYDB(a)
		vvv.Value = vv
	} else {
		vvv.Value = v.v.toYDB(a).Value
	}

	return vvv
}

func OptionalValue(v V) *optionalValue {
	return &optionalValue{
		t: Optional(v.Type()),
		v: v,
	}
}
