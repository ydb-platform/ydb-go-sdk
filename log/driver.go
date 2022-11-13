package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes trace.Driver with logging events from details
func Driver(l Logger, details trace.Details) (t trace.Driver) {
	return structural.Driver(Structural(l), details)
}
