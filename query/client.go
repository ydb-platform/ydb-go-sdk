package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Client interface {
	// Do provide the best effort for execute operation.
	//
	// Do implements internal busy loop until one of the following conditions is met:
	// - deadline was canceled or deadlined
	// - retry operation returned nil as error
	//
	// Warning: if context without deadline or cancellation func than Do can run indefinitely.
	Do(ctx context.Context, op Operation, opts ...options.DoOption) error

	// DoTx provide the best effort for execute transaction.
	//
	// DoTx implements internal busy loop until one of the following conditions is met:
	// - deadline was canceled or deadlined
	// - retry operation returned nil as error
	//
	// DoTx makes auto selector (with TransactionSettings, by default - SerializableReadWrite), commit and
	// rollback (on error) of transaction.
	//
	// If op TxOperation returns nil - transaction will be committed
	// If op TxOperation return non nil - transaction will be rollback
	// Warning: if context without deadline or cancellation func than DoTx can run indefinitely
	DoTx(ctx context.Context, op TxOperation, opts ...options.DoTxOption) error

	// ReadRow is a helper which read only one row from first result set in result
	//
	// ReadRow returns error if result contains more than one result set or more than one row
	//
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	ReadRow(ctx context.Context, query string, opts ...options.ExecuteOption) (Row, error)
}

type (
	// Operation is the interface that holds an operation for retry.
	// if Operation returns not nil - operation will retry
	// if Operation returns nil - retry loop will break
	Operation func(ctx context.Context, s Session) error

	// TxOperation is the interface that holds an operation for retry.
	// if TxOperation returns not nil - operation will retry
	// if TxOperation returns nil - retry loop will break
	TxOperation func(ctx context.Context, tx TxActor) error

	ClosableSession interface {
		closer.Closer

		Session
	}
	bothDoAndDoTxOption interface {
		options.DoOption
		options.DoTxOption
	}
)

func WithIdempotent() bothDoAndDoTxOption {
	return options.WithIdempotent()
}

func WithTrace(t *trace.Query) bothDoAndDoTxOption {
	return options.WithTrace(t)
}

func WithLabel(lbl string) bothDoAndDoTxOption {
	return options.WithLabel(lbl)
}

// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithRetryBudget(b budget.Budget) bothDoAndDoTxOption {
	return options.WithRetryBudget(b)
}
