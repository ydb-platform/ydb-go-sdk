package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logs/traces"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scripting returns trace.Scripting with logging events from details
func Scripting(l Logger, details trace.Details, opts ...traces.Option) (t trace.Scripting) {
	return traces.Scripting(newAdapter(l), details, opts...)
}
