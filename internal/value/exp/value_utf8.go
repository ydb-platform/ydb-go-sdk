package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type utf8Value struct {
	v string
}

func (v *utf8Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *utf8Value) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (*utf8Value) getType() T {
	return TypeUTF8
}

func (*utf8Value) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeUTF8]
}

func (v *utf8Value) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func UTF8Value(v string) *utf8Value {
	return &utf8Value{v: v}
}
