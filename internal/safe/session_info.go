package safe

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func SessionInfo[PT pool.ItemConstraint[T], T any](item PT) trace.SessionInfo {
	// PT is *T; compare with nil before interface conversion to avoid typed nil.
	if item == nil {
		return nil
	}

	s, ok := any(item).(trace.SessionInfo)
	if !ok {
		return nil
	}

	return s
}
