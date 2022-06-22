package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type optionalValue struct {
	t T
	v V
}

func (v *optionalValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *optionalValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (v *optionalValue) getType() T {
	return v.t
}

func (v *optionalValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	return v.t.toYDB(a)
}

func (v *optionalValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	if _, opt := v.v.(*optionalValue); opt {
		vv := a.Nested()
		vv.NestedValue = v.v.toYDBValue(a)
		vvv.Value = vv
	} else {
		vvv.Value = v.v.toYDBValue(a).Value
	}

	return vvv
}

func OptionalValue(v V) *optionalValue {
	return &optionalValue{
		t: Optional(v.getType()),
		v: v,
	}
}
