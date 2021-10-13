package table

import (
	"context"
)

// RetryOperation is the interface that holds an operation for retry.
type RetryOperation func(context.Context, Session) (err error)

type Client interface {
	RetryIdempotent(ctx context.Context, op RetryOperation) (err error)
	RetryNonIdempotent(ctx context.Context, op RetryOperation) (err error)
	Close(ctx context.Context) error
}
