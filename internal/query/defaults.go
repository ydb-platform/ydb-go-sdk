package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
)

var defaultSessionTrace = &traceSession{
	onCreate: func(ctx context.Context, functionID stack.Caller) func(*Session, error) {
		return func(session *Session, err error) {
		}
	},
	onAttach: func(ctx context.Context, functionID stack.Caller, s *Session) func(err error) {
		return func(err error) {
		}
	},
	onClose: func(ctx context.Context, functionID stack.Caller, s *Session) func(err error) {
		return func(err error) {
		}
	},
}
