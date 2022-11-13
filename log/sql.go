package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(l Logger, details trace.Details, opts ...structural.Option) (t trace.DatabaseSQL) {
	return structural.DatabaseSQL(Structural(l), details, opts...)
}
