package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type doubleValue struct {
	v float64
}

func (v *doubleValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *doubleValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (*doubleValue) getType() T {
	return TypeDouble
}

func (*doubleValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeDouble]
}

func (v *doubleValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Double()
	if v != nil {
		vv.DoubleValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DoubleValue(v float64) *doubleValue {
	return &doubleValue{v: v}
}
