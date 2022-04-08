package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Ratelimiter(l Logger, details trace.Details) (t trace.Ratelimiter) {
	if details&trace.RatelimiterEvents == 0 {
		return
	}
	_ = l.WithName(`ratelimiter`)
	return t
}
