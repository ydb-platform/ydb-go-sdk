package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Discovery makes trace.Discovery with logging events from details
func Discovery(l Logger, details trace.Details) (t trace.Discovery) {
	return structural.Discovery(Structural(l), details)
}
