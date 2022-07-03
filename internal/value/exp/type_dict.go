package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type (
	dictType struct {
		k T
		v T
	}
)

func (v *dictType) toString(buffer *bytes.Buffer) {
	buffer.WriteString("Dict<")
	v.k.toString(buffer)
	buffer.WriteByte(',')
	v.v.toString(buffer)
	buffer.WriteByte('>')
}

func (v *dictType) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *dictType) equalsTo(rhs T) bool {
	vv, ok := rhs.(*dictType)
	if !ok {
		return false
	}
	if !v.k.equalsTo(vv.k) {
		return false
	}
	if !v.v.equalsTo(vv.v) {
		return false
	}
	return true
}

func (v *dictType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeDict := a.TypeDict()

	typeDict.DictType = a.Dict()

	typeDict.DictType.Key = v.k.toYDB(a)
	typeDict.DictType.Payload = v.v.toYDB(a)

	t.Type = typeDict

	return t
}

func Dict(key, value T) (v *dictType) {
	return &dictType{
		k: key,
		v: value,
	}
}
