package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logs/traces"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Retry returns trace.Retry with logging events from details
func Retry(l Logger, details trace.Details) (t trace.Retry) {
	return traces.Retry(newAdapter(l), details)
}
