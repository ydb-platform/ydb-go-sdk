package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type tupleValue struct {
	t     T
	items []V
}

func (v *tupleValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *tupleValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (v *tupleValue) getType() T {
	return v.t
}

func (v *tupleValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	return v.t.toYDB(a)
}

func (v *tupleValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	var items []V
	if v != nil {
		items = v.items
	}
	vvv := a.Value()

	for _, vv := range items {
		vvv.Items = append(vvv.Items, vv.toYDBValue(a))
	}

	return vvv
}

func TupleValue(values ...V) *tupleValue {
	var (
		tupleItems []T
	)
	for _, v := range values {
		tupleItems = append(tupleItems, v.getType())
	}
	return &tupleValue{
		t:     Tuple(tupleItems...),
		items: values,
	}
}
