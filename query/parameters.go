package query

import (
	"sort"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

// Parameters
type (
	queryParams map[string]Value
	Parameter   interface {
		Name() string
		Value() Value
	}
	parameter struct {
		name  string
		value Value
	}
	Parameters struct {
		m queryParams
	}
	parametersOption struct {
		params *Parameters
	}
)

func (opt *parametersOption) applyTxExecuteOption(o *txExecuteSettings) {
	o.params = opt.params
}

func (opt *parametersOption) applyExecuteOption(o *executeSettings) {
	o.params = opt.params
}

func (p parameter) Name() string {
	return p.name
}

func (p parameter) Value() Value {
	return p.value
}

func (params *Parameters) ToYDB(a *allocator.Allocator) map[string]*Ydb.TypedValue {
	if params == nil || params.m == nil {
		return nil
	}
	return params.m.toYDB(a)
}

func (qp queryParams) toYDB(a *allocator.Allocator) map[string]*Ydb.TypedValue {
	params := make(map[string]*Ydb.TypedValue, len(qp))
	for k, v := range qp {
		params[k] = value.ToYDB(v, a)
	}
	return params
}

func (params *Parameters) Params() queryParams {
	if params == nil {
		return nil
	}
	return params.m
}

func (params *Parameters) Count() int {
	if params == nil {
		return 0
	}
	return len(params.m)
}

func (params *Parameters) Each(it func(name string, v Value)) {
	if params == nil {
		return
	}
	for key, v := range params.m {
		it(key, v)
	}
}

func (params *Parameters) names() []string {
	if params == nil {
		return nil
	}
	names := make([]string, 0, len(params.m))
	for k := range params.m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

func (params *Parameters) String() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteByte('{')
	for i, name := range params.names() {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteByte('"')
		buffer.WriteString(name)
		buffer.WriteString("\":")
		buffer.WriteString(params.m[name].Yql())
	}
	buffer.WriteByte('}')
	return buffer.String()
}

func (params *Parameters) Add(parameters ...Parameter) {
	for _, param := range parameters {
		params.m[param.Name()] = param.Value()
	}
}

func Param(name string, v Value) Parameter {
	switch len(name) {
	case 0:
		panic("empty name")
	default:
		if name[0] != '$' {
			panic("parameter name must be started from $")
		}
	}
	return &parameter{
		name:  name,
		value: v,
	}
}
