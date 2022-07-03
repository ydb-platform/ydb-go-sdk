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
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *dictValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *dictValue) Type() T {
	return v.t
}

func (v *dictValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var values []DictValueField
	if v != nil {
		values = v.values
	}
	vvv := a.Value()

	for _, vv := range values {
		pair := a.Pair()

		pair.Key = vv.K.toYDB(a)
		pair.Payload = vv.V.toYDB(a)

		vvv.Pairs = append(vvv.Pairs, pair)
	}

	return vvv
}

func DictValue(values ...DictValueField) *dictValue {
	return &dictValue{
		t:      Dict(values[0].K.Type(), values[0].V.Type()),
		values: values,
	}
}
