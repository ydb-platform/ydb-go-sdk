package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type (
	DictValueField struct {
		K V
		V V
	}
	dictValue struct {
		t      T
		values []DictValueField
	}
)

func (v *dictValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *dictValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (v *dictValue) getType() T {
	return v.t
}

func (v *dictValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	return v.t.toYDB(a)
}

func (v *dictValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	var values []DictValueField
	if v != nil {
		values = v.values
	}
	vvv := a.Value()

	for _, vv := range values {
		pair := a.Pair()

		pair.Key = vv.K.toYDBValue(a)
		pair.Payload = vv.V.toYDBValue(a)

		vvv.Pairs = append(vvv.Pairs, pair)
	}

	return vvv
}

func DictValue(values ...DictValueField) *dictValue {
	return &dictValue{
		t:      Dict(values[0].K.getType(), values[0].V.getType()),
		values: values,
	}
}
