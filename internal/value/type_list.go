package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type listType struct {
	t T
}

func (v *listType) toString(buffer *bytes.Buffer) {
	buffer.WriteString("List<")
	v.t.toString(buffer)
	buffer.WriteString(">")
}

func (v *listType) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *listType) equalsTo(rhs T) bool {
	vv, ok := rhs.(*listType)
	if !ok {
		return false
	}
	return v.t.equalsTo(vv.t)
}

func (v *listType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	list := a.List()

	list.Item = v.t.toYDB(a)

	typeList := a.TypeList()
	typeList.ListType = list

	t.Type = typeList

	return t
}

func List(t T) *listType {
	return &listType{
		t: t,
	}
}
