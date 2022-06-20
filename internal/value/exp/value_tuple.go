package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type tupleValue struct {
	items []V
}

func (v *tupleValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	var items []V
	if v != nil {
		items = v.items
	}
	t := a.Type()

	typeTuple := a.TypeTuple()

	typeTuple.TupleType = a.Tuple()

	for _, vv := range items {
		typeTuple.TupleType.Elements = append(typeTuple.TupleType.Elements, vv.toYDBType(a))
	}

	t.Type = typeTuple

	return t
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

func TupleValue(v ...V) *tupleValue {
	return &tupleValue{items: v}
}
