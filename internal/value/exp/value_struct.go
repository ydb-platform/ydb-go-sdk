package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type (
	StructValueField struct {
		Name string
		V    V
	}
	structValue struct {
		t      T
		values []V
	}
)

func (v *structValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *structValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (v *structValue) getType() T {
	return v.t
}

func (v *structValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	return v.t.toYDB(a)
}

func (v structValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	for _, vv := range v.values {
		vvv.Items = append(vvv.Items, vv.toYDBValue(a))
	}

	return vvv
}

func StructValue(fields ...StructValueField) *structValue {
	var (
		structFields []StructField
		values       []V
	)
	for _, field := range fields {
		structFields = append(structFields, StructField{field.Name, field.V.getType()})
		values = append(values, field.V)
	}
	return &structValue{
		t:      Struct(structFields...),
		values: values,
	}
}
