package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
)

// Operation is the interface that holds an operation for retry.
// if Operation returns not nil - operation will retry
// if Operation returns nil - retry loop will break
type Operation func(ctx context.Context, s Session) error

// TxOperation is the interface that holds an operation for retry.
// if TxOperation returns not nil - operation will retry
// if TxOperation returns nil - retry loop will break
type TxOperation func(ctx context.Context, tx TransactionActor) error

type ClosableSession interface {
	closer.Closer
	Session
}

type Client interface {
	closer.Closer

	// CreateSession returns session or error for manually control of session lifecycle
	// CreateSession do not provide retry loop for failed create session requests.
	// Best effort policy may be implements with outer retry loop includes CreateSession call
	CreateSession(ctx context.Context, opts ...Option) (s ClosableSession, err error)

	// Do provide the best effort for execute operation
	// Do implements internal busy loop until one of the following conditions is met:
	// - deadline was canceled or deadlined
	// - retry operation returned nil as error
	// Warning: if context without deadline or cancellation func than Do can run indefinitely
	Do(ctx context.Context, op Operation, opts ...Option) error

	// DoTx provide the best effort for execute transaction
	// DoTx implements internal busy loop until one of the following conditions is met:
	// - deadline was canceled or deadlined
	// - retry operation returned nil as error
	// DoTx makes auto begin, commit and rollback of transaction
	// If op TxOperation returns nil - transaction will be committed
	// If op TxOperation return non nil - transaction will be rollback
	// Warning: if context without deadline or cancellation func than DoTx can run indefinitely
	DoTx(ctx context.Context, op TxOperation, opts ...Option) error
}
