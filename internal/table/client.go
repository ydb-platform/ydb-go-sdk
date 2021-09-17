package table

import (
	"context"
)

type Client interface {
	CreateSession(ctx context.Context) (*Session, error)
	Retry(ctx context.Context, retryNoIdempotent bool, op RetryOperation) (err error, issues []error)
	Close(ctx context.Context) error
}
