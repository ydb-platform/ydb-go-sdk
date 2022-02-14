package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Scheme(log Logger, details trace.Details) (t trace.Scheme) {
	if details&trace.SchemeEvents != 0 {
		// nolint:staticcheck
		log = log.WithName(`scheme`)
	}
	return t
}
