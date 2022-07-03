package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type emptyListType struct{}

func (v emptyListType) toString(buffer *bytes.Buffer) {
	buffer.WriteString("List<>")
}

func (v emptyListType) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (emptyListType) equalsTo(rhs T) bool {
	_, ok := rhs.(emptyListType)
	return ok
}

func (emptyListType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	t.Type = a.TypeEmptyList()

	return t
}

func EmptyList() emptyListType {
	return emptyListType{}
}
