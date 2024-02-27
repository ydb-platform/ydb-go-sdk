package params

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

type (
	parameter struct {
		parent Builder
		name   string
		value  value.Value
	}
	Builder    []*parameter
	Parameters interface {
		ToYDB(a *allocator.Allocator) map[string]*Ydb.TypedValue
		String() string
	}
	parameters []*parameter
)

func (parameters parameters) String() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteByte('{')
	for i := range parameters {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteByte('"')
		buffer.WriteString(parameters[i].name)
		buffer.WriteString("\":")
		buffer.WriteString(parameters[i].value.Yql())
	}
	buffer.WriteByte('}')

	return buffer.String()
}

func (parameters parameters) ToYDB(a *allocator.Allocator) map[string]*Ydb.TypedValue {
	params := make(map[string]*Ydb.TypedValue, len(parameters))
	for _, param := range parameters {
		params[param.name] = value.ToYDB(param.value, a)
	}

	return params
}

func Nil() Parameters {
	return parameters{}
}

func (b Builder) Build() Parameters {
	return parameters(b)
}

func (b Builder) Param(name string) *parameter {
	return &parameter{
		parent: b,
		name:   name,
		value:  nil,
	}
}

func (param *parameter) Text(v string) Builder {
	param.value = value.TextValue(v)
	param.parent = append(param.parent, param)

	return param.parent
}

func (param *parameter) Uint64(v uint64) Builder {
	param.value = value.Uint64Value(v)
	param.parent = append(param.parent, param)

	return param.parent
}
