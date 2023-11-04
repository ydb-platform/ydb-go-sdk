package trace

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"

// default behavior of driver not generate functionID in traces
var skipFunctionID = true

func FunctionID(depth int) string {
	if skipFunctionID {
		return ""
	}
	return stack.Record(depth+1, stack.Lambda(false), stack.FileName(false))
}

func EnableFunctionID() {
	skipFunctionID = false
}
