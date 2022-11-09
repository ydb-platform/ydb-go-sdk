package structural

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Ratelimiter returns trace.Ratelimiter with logging events from details
func Ratelimiter(l Logger, details trace.Details) (t trace.Ratelimiter) {
	if details&trace.RatelimiterEvents == 0 {
		return
	}
	_ = l.WithName(`ratelimiter`)
	return t
}
