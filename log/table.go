package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logs/traces"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Table with logging events from details
func Table(l Logger, details trace.Details, opts ...traces.Option) (t trace.Table) {
	return traces.Table(newAdapter(l), details, opts...)
}
