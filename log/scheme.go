package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logs/traces"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scheme returns trace.Scheme with logging events from details
func Scheme(l Logger, details trace.Details) (t trace.Scheme) {
	return traces.Scheme(newAdapter(l), details)
}
