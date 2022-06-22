package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type floatValue struct {
	v float32
}

func (v *floatValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *floatValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (*floatValue) getType() T {
	return TypeFloat
}

func (*floatValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeFloat]
}

func (v *floatValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
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
