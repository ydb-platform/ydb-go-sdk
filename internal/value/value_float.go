package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type floatValue struct {
	v float32
}

func (v *floatValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *floatValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*floatValue) Type() T {
	return TypeFloat
}

func (v *floatValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Float()
	if v != nil {
		vv.FloatValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func FloatValue(v float32) *floatValue {
	return &floatValue{v: v}
}
