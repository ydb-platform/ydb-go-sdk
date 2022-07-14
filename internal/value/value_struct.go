package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
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
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *structValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *structValue) Type() T {
	return v.t
}

func (v structValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	for _, vv := range v.values {
		vvv.Items = append(vvv.Items, vv.toYDB(a))
	}

	return vvv
}

func StructValue(fields ...StructValueField) *structValue {
	var (
		structFields = make([]StructField, 0, len(fields))
		values       = make([]V, 0, len(fields))
	)
	for _, field := range fields {
		structFields = append(structFields, StructField{field.Name, field.V.Type()})
		values = append(values, field.V)
	}
	return &structValue{
		t:      Struct(structFields...),
		values: values,
	}
}
