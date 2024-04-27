package params

type (
	Builder struct {
		params Parameters
	}
)

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
