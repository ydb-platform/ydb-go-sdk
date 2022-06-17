package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type optionalValue struct {
	v V
}

func (v optionalValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeOptional := a.TypeOptional()

	typeOptional.OptionalType = a.Optional()

	typeOptional.OptionalType.Item = v.v.toYDBType(a)

	t.Type = typeOptional

	return t
}

func (v optionalValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	if _, opt := v.v.(optionalValue); opt {
		vv := a.NestedValue()
		vv.NestedValue = v.v.toYDBValue(a)
		vvv.Value = vv
	} else {
		vvv.Value = v.v.toYDBValue(a).Value
	}

	return vvv
}

func OptionalValue(v V) optionalValue {
	return optionalValue{
		v: v,
	}
}
