package params

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type (
	Builder struct {
		params Parameters
	}
)

func (b Builder) append(name string, value value.Value) {
	b.params = append(b.params, &Parameter{
		parent: b,
		name:   name,
		value:  value,
	})
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
