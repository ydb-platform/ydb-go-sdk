package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type listValue []V

func (v listValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	switch {
	case len(v) > 0:
		list := a.List()

		list.Item = v[0].toYDBType(a)

		typeList := a.TypeList()
		typeList.ListType = list

		t.Type = typeList
	default:
		typeList := a.EmptyTypeList()

		t.Type = typeList
	}

	return t
}

func (v listValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	for _, vv := range v {
		vvv.Items = append(vvv.Items, vv.toYDBValue(a))
	}

	return vvv
}

func ListValue(v ...V) listValue {
	return listValue(v)
}
