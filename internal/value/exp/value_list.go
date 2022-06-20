package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type listValue struct {
	items []V
}

func (v *listValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	var items []V
	if v != nil {
		items = v.items
	}
	t := a.Type()

	switch {
	case len(items) > 0:
		list := a.List()

		list.Item = items[0].toYDBType(a)

		typeList := a.TypeList()
		typeList.ListType = list

		t.Type = typeList
	default:
		typeList := a.TypeEmptyList()

		t.Type = typeList
	}

	return t
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

func ListValue(v ...V) *listValue {
	return &listValue{items: v}
}
