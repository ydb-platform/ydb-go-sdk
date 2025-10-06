package params

type (
	Builder struct {
		params Params
	}
)

func (b Builder) Build() *Params {
	return &b.params
}

func (b Builder) build() *Params {
	return &b.params
}

func (b Builder) Param(name string) *Parameter {
	return &Parameter{
		parent: b,
		name:   name,
		value:  nil,
	}
}

// Такое решение тоже неплохое, но xiter.Seq2 не работает с Go 1.22, придется менять версию
func (b Builder) AddFromRange(params Parameters) Builder {
	for name, value := range params.Range() {
		b = b.Param(name).Any(value)
	}
	return b
}
