package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type (
	TupleType struct {
		items []T
	}
)

func (v *TupleType) toString(buffer *bytes.Buffer) {
	buffer.WriteString("Tuple<")
	for i, t := range v.items {
		if i > 0 {
			buffer.WriteByte(',')
		}
		t.toString(buffer)
	}
	buffer.WriteByte('>')
}

func (v *TupleType) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (v *TupleType) equalsTo(rhs T) bool {
	vv, ok := rhs.(*TupleType)
	if !ok {
		return false
	}
	if len(v.items) != len(vv.items) {
		return false
	}
	for i := range v.items {
		if !v.items[i].equalsTo(vv.items[i]) {
			return false
		}
	}
	return true
}

func (v *TupleType) toYDB(a *allocator.Allocator) *Ydb.Type {
	var items []T
	if v != nil {
		items = v.items
	}
	t := a.Type()

	typeTuple := a.TypeTuple()

	typeTuple.TupleType = a.Tuple()

	for _, vv := range items {
		typeTuple.TupleType.Elements = append(typeTuple.TupleType.Elements, vv.toYDB(a))
	}

	t.Type = typeTuple

	return t
}

func Tuple(items ...T) (v *TupleType) {
	return &TupleType{
		items: items,
	}
}
