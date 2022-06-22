package value

import (
	"bytes"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type listValue struct {
	t     T
	items []V
}

func (v *listValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *listValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (v *listValue) getType() T {
	return v.t
}

func (v *listValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	return v.t.toYDB(a)
}

func (v *listValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
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

func ListValue(items ...V) *listValue {
	var t T
	switch {
	case len(items) > 0:
		t = List(items[0].getType())
	default:
		t = EmptyList()
	}

	for _, v := range items {
		if !v.getType().equalsTo(v.getType()) {
			panic(fmt.Sprintf("different types of items: %v", items))
		}
	}
	return &listValue{
		t:     t,
		items: items,
	}
}
