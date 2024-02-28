package query

import "context"

type (
	TxIdentifier interface {
		ID() string
	}

	TxActor interface {
		TxIdentifier

		// Execute executes query.
		//
		// Execute used by default:
		// - DefaultTxControl
		// - flag WithKeepInCache(true) if params is not empty.
		Execute(ctx context.Context, query string, opts ...TxExecuteOption) (r Result, err error)
	}

	Transaction interface {
		TxActor

		CommitTx(ctx context.Context) (err error)
		Rollback(ctx context.Context) (err error)
	}
)
