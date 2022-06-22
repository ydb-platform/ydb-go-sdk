package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type jsonValue struct {
	v string
}

func (v *jsonValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *jsonValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (*jsonValue) getType() T {
	return TypeJSON
}

func (*jsonValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeJSON]
}

func (v *jsonValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func JSONValue(v string) *jsonValue {
	return &jsonValue{v: v}
}
