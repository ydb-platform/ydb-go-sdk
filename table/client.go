package table

import (
	"context"
)

// RetryOperation is the interface that holds an operation for retry.
type RetryOperation func(context.Context, Session) (err error)

type Client interface {
	CreateSession(ctx context.Context) (Session, error)
	RetryIdempotent(ctx context.Context, op RetryOperation) (err error, issues []error)
	RetryNonIdempotent(ctx context.Context, op RetryOperation) (err error, issues []error)
	Close(ctx context.Context) error
}
