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
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *variantValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *variantValue) Type() T {
	return v.t
}

func (v *variantValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	nested := a.Nested()
	nested.NestedValue = v.v.toYDB(a)

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

func VariantValueStruct(v V, idx uint32) *variantValue {
	if _, ok := v.(*structValue); !ok {
		panic("value must be a struct type")
	}
	return &variantValue{
		t: &variantType{
			t:  v.Type(),
			tt: variantTypeStruct,
		},
		v:   v,
		idx: idx,
	}
}

func VariantValueTuple(v V, idx uint32) *variantValue {
	if _, ok := v.(*tupleValue); !ok {
		panic("value must be a tuple type")
	}
	return &variantValue{
		t: &variantType{
			t:  v.Type(),
			tt: variantTypeTuple,
		},
		v:   v,
		idx: idx,
	}
}
