package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type tupleValue struct {
	t     T
	items []V
}

func (v *tupleValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *tupleValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *tupleValue) Type() T {
	return v.t
}

func (v *tupleValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var items []V
	if v != nil {
		items = v.items
	}
	vvv := a.Value()

	for _, vv := range items {
		vvv.Items = append(vvv.Items, vv.toYDB(a))
	}

	return vvv
}

func TupleValue(values ...V) *tupleValue {
	tupleItems := make([]T, 0, len(values))
	for _, v := range values {
		tupleItems = append(tupleItems, v.Type())
	}
	return &tupleValue{
		t:     Tuple(tupleItems...),
		items: values,
	}
}
