package builder

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type (
	parameter struct {
		parent ParamsBuilder
		name   string
		value  value.Value
	}
	ParamsBuilder []*parameter
	Parameters    interface {
		ToYDB(a *allocator.Allocator) map[string]*Ydb.TypedValue
	}
	parameters []*parameter
)

func (parameters parameters) ToYDB(a *allocator.Allocator) map[string]*Ydb.TypedValue {
	params := make(map[string]*Ydb.TypedValue, len(parameters))
	for _, param := range parameters {
		params[param.name] = value.ToYDB(param.value, a)
	}

	return params
}

func Params() ParamsBuilder {
	return ParamsBuilder{}
}

func (b ParamsBuilder) Build() Parameters {
	return parameters(b)
}

func (b ParamsBuilder) Param(name string) *parameter {
	return &parameter{
		parent: b,
		name:   name,
		value:  nil,
	}
}

func (param *parameter) Text(v string) ParamsBuilder {
	param.value = value.TextValue(v)
	param.parent = append(param.parent, param)

	return param.parent
}

func (param *parameter) Uint64(v uint64) ParamsBuilder {
	param.value = value.Uint64Value(v)
	param.parent = append(param.parent, param)

	return param.parent
}
