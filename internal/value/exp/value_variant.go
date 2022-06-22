package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type variantValue struct {
	t   T
	v   V
	idx uint32
}

func (v *variantValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *variantValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (v *variantValue) getType() T {
	return v.t
}

func (v *variantValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	return v.t.toYDB(a)
}

func (v *variantValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	nested := a.Nested()
	nested.NestedValue = v.v.toYDBValue(a)

	vvv.Value = nested
	vvv.VariantIndex = v.idx

	return vvv
}

func VariantValue(v V, idx uint32, t T) *variantValue {
	return &variantValue{
		t:   Variant(t),
		v:   v,
		idx: idx,
	}
}
