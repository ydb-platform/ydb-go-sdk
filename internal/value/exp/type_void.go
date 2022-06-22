package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type voidType struct{}

func (v voidType) toString(buffer *bytes.Buffer) {
	buffer.WriteString(v.String())
}

func (v voidType) String() string {
	return "Void"
}

var _voidType = &Ydb.Type{
	Type: &Ydb.Type_VoidType{},
}

func (v voidType) equalsTo(rhs T) bool {
	_, ok := rhs.(voidType)
	return ok
}

func (voidType) toYDB(*allocator.Allocator) *Ydb.Type {
	return _voidType
}

func Void() voidType {
	return voidType{}
}
