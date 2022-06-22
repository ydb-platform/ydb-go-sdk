package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type jsonDocumentValue struct {
	v string
}

func (v *jsonDocumentValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *jsonDocumentValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (*jsonDocumentValue) getType() T {
	return TypeJSONDocument
}

func (*jsonDocumentValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeJSONDocument]
}

func (v *jsonDocumentValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func JSONDocumentValue(v string) *jsonDocumentValue {
	return &jsonDocumentValue{v: v}
}
