package stack

type Caller interface {
	String() string
}

var _ Caller = FunctionIDType("")

type FunctionIDType string

func (id FunctionIDType) String() string {
	return string(id)
}

func FunctionID(id string, opts ...recordOption) Caller {
	if id != "" {
		return FunctionIDType(id)
	}

	return Call(1, opts...)
}
