package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type voidValue struct{}

var (
	_voidType = &Ydb.Type{
		Type: &Ydb.Type_VoidType{},
	}
	_voidValue = &Ydb.Value{
		Value: new(Ydb.Value_NullFlagValue),
	}
)

func (v voidValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	return _voidType
}

func (v voidValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	return _voidValue
}

func VoidValue() voidValue {
	return voidValue{}
}
