package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logs/traces"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(l Logger, details trace.Details, opts ...traces.Option) (t trace.DatabaseSQL) {
	return traces.DatabaseSQL(newAdapter(l), details, opts...)
}
