package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logs/traces"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Ratelimiter returns trace.Ratelimiter with logging events from details
func Ratelimiter(l Logger, details trace.Details) (t trace.Ratelimiter) {
	return traces.Ratelimiter(newAdapter(l), details)
}
