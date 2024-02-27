package params

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/value"

type (
	Parameter struct {
		parent Builder
		name   string
		value  value.Value
	}
)

func Named(name string, value value.Value) *Parameter {
	return &Parameter{
		name:  name,
		value: value,
	}
}

func (p *Parameter) Name() string {
	return p.name
}

func (p *Parameter) Value() value.Value {
	return p.value
}
