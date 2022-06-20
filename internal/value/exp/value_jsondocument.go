package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type jsondocumentValue struct {
	v string
}

func (*jsondocumentValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	typePrimitive := a.TypePrimitive()
	typePrimitive.TypeId = Ydb.Type_JSON_DOCUMENT

	t := a.Type()
	t.Type = typePrimitive

	return t
}

func (v *jsondocumentValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func JSONDocumentValue(v string) *jsondocumentValue {
	return &jsondocumentValue{v: v}
}
