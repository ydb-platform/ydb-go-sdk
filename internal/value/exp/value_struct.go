package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type (
	structField struct {
		name string
		v    V
	}
	structValue []structField
)

func (v structValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeStruct := a.TypeStruct()

	typeStruct.StructType = a.Struct()

	for _, vv := range v {
		structMember := a.StructMember()
		structMember.Name = vv.name
		structMember.Type = vv.v.toYDBType(a)
		typeStruct.StructType.Members = append(
			typeStruct.StructType.Members,
			structMember,
		)
	}

	t.Type = typeStruct

	return t
}

func (v structValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	for _, vv := range v {
		vvv.Items = append(vvv.Items, vv.v.toYDBValue(a))
	}

	return vvv
}

func StructField(name string, value V) structField {
	return structField{name, value}
}

func StructValue(v ...structField) structValue {
	return v
}
