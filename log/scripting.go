package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scripting returns trace.Scripting with logging events from details
func Scripting(l Logger, details trace.Details, opts ...structural.Option) (t trace.Scripting) {
	return structural.Scripting(Structural(l), details, opts...)
}
