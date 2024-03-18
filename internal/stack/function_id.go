package stack

type Caller interface {
	FunctionID() string
}

var _ Caller = functionID("")

type functionID string

func (id functionID) FunctionID() string {
	return string(id)
}

func FunctionID(id string) Caller {
	if id != "" {
		return functionID(id)
	}

	return Call(1)
}
