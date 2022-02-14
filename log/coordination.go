package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Coordination(log Logger, details trace.Details) (t trace.Coordination) {
	if details&trace.CoordinationEvents != 0 {
		// nolint:staticcheck
		log = log.WithName(`coordination`)
	}
	return t
}
