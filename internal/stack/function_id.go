package stack

type Caller interface {
	String() string
}

var _ Caller = FunctionID("")

type FunctionID string

func (id FunctionID) String() string {
	return string(id)
}

func RuntimeFunctionID(id string, opts ...recordOption) Caller {
	if id != "" {
		return FunctionID(id)
	}

	return Call(1, opts...)
}
