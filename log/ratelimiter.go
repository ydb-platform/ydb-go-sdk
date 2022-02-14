package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Ratelimiter(log Logger, details trace.Details) (t trace.Ratelimiter) {
	if details&trace.RatelimiterEvents != 0 {
		// nolint:staticcheck
		log = log.WithName(`ratelimiter`)
	}
	return t
}
