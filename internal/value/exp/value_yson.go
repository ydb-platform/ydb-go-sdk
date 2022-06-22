package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type ysonValue struct {
	v string
}

func (v *ysonValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *ysonValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (*ysonValue) getType() T {
	return TypeYSON
}

func (*ysonValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeYSON]
}

func (v *ysonValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func YSONValue(v string) *ysonValue {
	return &ysonValue{v: v}
}
