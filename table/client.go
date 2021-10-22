package table

import (
	"context"
)

// RetryOperation is the interface that holds an operation for retry.
type RetryOperation func(context.Context, Session) (err error)

type Client interface {
	Close(ctx context.Context) error

	// RetryIdempotent retries the retry operation as a idempotent
	// operation (i.e. the operation can't change the state of the database)
	// while the operation returns an error
	//
	// This is an experimental API that can be changed without changing
	// the major version.
	RetryIdempotent(ctx context.Context, op RetryOperation) (err error)

	// RetryNonIdempotent retries the retry operation as a non-idempotent
	// operation (i.e. the operation can change the state of the database)
	// while the operation returns an error
	//
	// This is an experimental API that can be changed without changing
	// the major version.
	RetryNonIdempotent(ctx context.Context, op RetryOperation) (err error)
}
