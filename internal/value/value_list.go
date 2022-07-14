package value

import (
	"bytes"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type listValue struct {
	t     T
	items []V
}

func (v *listValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *listValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *listValue) Type() T {
	return v.t
}

func (v *listValue) toYDB(a *allocator.Allocator) *Ydb.Value {
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

func ListValue(items ...V) *listValue {
	var t T
	switch {
	case len(items) > 0:
		t = List(items[0].Type())
	default:
		t = EmptyList()
	}

	for _, v := range items {
		if !v.Type().equalsTo(v.Type()) {
			panic(fmt.Sprintf("different types of items: %v", items))
		}
	}
	return &listValue{
		t:     t,
		items: items,
	}
}
