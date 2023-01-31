package traces

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scheme returns trace.Scheme with logging events from details
func Scheme(l logs.Logger, details trace.Details) (t trace.Scheme) {
	if details&trace.SchemeEvents == 0 {
		return
	}
	_ = newLogger(l, "scheme")
	return t
}
