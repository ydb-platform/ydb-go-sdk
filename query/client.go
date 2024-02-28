package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
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
	Do(ctx context.Context, op Operation, opts ...DoOption) error

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
	DoTx(ctx context.Context, op TxOperation, opts ...DoTxOption) error
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

	DoOption interface {
		applyDoOption(o *DoOptions)
	}

	DoOptions struct {
		Label        string
		Idempotent   bool
		RetryOptions []retry.Option
		Trace        *trace.Query
	}

	DoTxOption interface {
		applyDoTxOption(o *DoTxOptions)
	}

	DoTxOptions struct {
		DoOptions

		TxSettings TransactionSettings
	}
)
