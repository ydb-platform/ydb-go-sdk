package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type tupleValue []V

func (v tupleValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeTuple := a.TypeTuple()

	typeTuple.TupleType = a.Tuple()

	for _, vv := range v {
		typeTuple.TupleType.Elements = append(typeTuple.TupleType.Elements, vv.toYDBType(a))
	}

	t.Type = typeTuple

	return t
}

func (v tupleValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	for _, vv := range v {
		vvv.Items = append(vvv.Items, vv.toYDBValue(a))
	}

	return vvv
}

func TupleValue(v ...V) tupleValue {
	return tupleValue(v)
}
