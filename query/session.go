package query

import (
	"context"
)

type (
	SessionInfo interface {
		ID() string
		NodeID() int64
		Status() SessionStatus
	}

	Session interface {
		SessionInfo

		// Execute executes query.
		//
		// Execute used by default:
		// - DefaultTxControl
		// - flag WithKeepInCache(true) if params is not empty.
		Execute(ctx context.Context, query string, opts ...ExecuteOption) (tx Transaction, r Result, err error)

		Begin(ctx context.Context, txSettings TransactionSettings) (Transaction, error)
	}
)
