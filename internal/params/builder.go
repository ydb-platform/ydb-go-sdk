package params

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

type (
	Builder struct {
		params Parameters
	}
	Parameters []*Parameter
)

func (p *Parameters) String() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteByte('{')
	if p != nil {
		for i, param := range *p {
			if i != 0 {
				buffer.WriteByte(',')
			}
			buffer.WriteByte('"')
			buffer.WriteString(param.name)
			buffer.WriteString("\":")
			buffer.WriteString(param.value.Yql())
		}
	}
	buffer.WriteByte('}')

	return buffer.String()
}

func (p *Parameters) ToYDB(a *allocator.Allocator) map[string]*Ydb.TypedValue {
	parameters := make(map[string]*Ydb.TypedValue, len(*p))
	for _, param := range *p {
		parameters[param.name] = value.ToYDB(param.value, a)
	}

	return parameters
}

func (p *Parameters) Each(it func(name string, v value.Value)) {
	if p == nil {
		return
	}
	for _, p := range *p {
		it(p.name, p.value)
	}
}

func (p *Parameters) Count() int {
	if p == nil {
		return 0
	}

	return len(*p)
}

func (p *Parameters) Add(params ...*Parameter) {
	for _, param := range params {
		*p = append(*p, param)
	}
}

func Nil() *Parameters {
	return &Parameters{}
}

func (b Builder) Build() *Parameters {
	return &b.params
}

func (b Builder) Param(name string) *Parameter {
	return &Parameter{
		parent: b,
		name:   name,
		value:  nil,
	}
}

func (p *Parameter) Text(v string) Builder {
	p.value = value.TextValue(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Uint64(v uint64) Builder {
	p.value = value.Uint64Value(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}
