package stack

type caller interface {
	FunctionID() string
}

var _ caller = functionID("")

type functionID string

func (id functionID) FunctionID() string {
	return string(id)
}

func FunctionID(id string) caller {
	if id != "" {
		return functionID(id)
	}
	return Call(1)
}
