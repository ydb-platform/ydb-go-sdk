package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type optionalType struct {
	t T
}

func (v *optionalType) toString(buffer *bytes.Buffer) {
	buffer.WriteString("Optional<")
	v.t.toString(buffer)
	buffer.WriteString(">")
}

func (v *optionalType) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *optionalType) equalsTo(rhs T) bool {
	vv, ok := rhs.(*optionalType)
	if !ok {
		return false
	}
	return v.t.equalsTo(vv.t)
}

func (v *optionalType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeOptional := a.TypeOptional()

	typeOptional.OptionalType = a.Optional()

	typeOptional.OptionalType.Item = v.t.toYDB(a)

	t.Type = typeOptional

	return t
}

func Optional(t T) *optionalType {
	return &optionalType{
		t: t,
	}
}
