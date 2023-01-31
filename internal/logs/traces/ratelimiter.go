package traces

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Ratelimiter returns trace.Ratelimiter with logging events from details
func Ratelimiter(l logs.Logger, details trace.Details) (t trace.Ratelimiter) {
	if details&trace.RatelimiterEvents == 0 {
		return
	}
	_ = newLogger(l, "ratelimiter")
	return t
}
