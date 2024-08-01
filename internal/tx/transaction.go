package tx

import "context"

// TODO: think about name
type Notificator interface {
	Identifier
	SessionID() string
	OnCompleted(f OnTransactionCompletedFunc)
	Rollback(ctx context.Context) error
}

type OnTransactionCompletedFunc func(transactionResult error)
