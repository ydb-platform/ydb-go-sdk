package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Coordination makes trace.Coordination with logging events from details
func Coordination(l Logger, details trace.Details) (t trace.Coordination) {
	return structural.Coordination(Structural(l), details)
}
