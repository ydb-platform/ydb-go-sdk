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
	ParamsBuilder []parameter
	Parameters    interface {
		ToYDB(a *allocator.Allocator) map[string]*Ydb.TypedValue
	}
	parameters []parameter
)

func (qp parameters) ToYDB(a *allocator.Allocator) map[string]*Ydb.TypedValue {
	params := make(map[string]*Ydb.TypedValue, len(qp))
	for _, param := range qp {
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

func (b ParamsBuilder) Param(name string) parameter {
	return parameter{
		parent: b,
		name:   name,
		value:  nil,
	}
}

func (p parameter) Text(v string) ParamsBuilder {
	p.value = value.TextValue(v)
	p.parent = append(p.parent, p)
	return p.parent
}

func (p parameter) Uint64(v uint64) ParamsBuilder {
	p.value = value.Uint64Value(v)
	p.parent = append(p.parent, p)
	return p.parent
}
