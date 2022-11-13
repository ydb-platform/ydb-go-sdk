package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scheme returns trace.Scheme with logging events from details
func Scheme(l Logger, details trace.Details) (t trace.Scheme) {
	return structural.Scheme(Structural(l), details)
}
