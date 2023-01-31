package traces

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Coordination makes trace.Coordination with logging events from details
func Coordination(l logs.Logger, details trace.Details) (t trace.Coordination) {
	if details&trace.CoordinationEvents == 0 {
		return
	}
	_ = newLogger(l, "coordination")
	return t
}
