package stack

type Caller interface {
	String() string
}

var _ Caller = functionID("")

type functionID string

func (id functionID) String() string {
	return string(id)
}

func FunctionID(id string) Caller {
	if id != "" {
		return functionID(id)
	}

	return Call(1)
}
