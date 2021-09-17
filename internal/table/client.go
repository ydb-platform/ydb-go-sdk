package table

import (
	"context"
)

type Client interface {
	CreateSession(ctx context.Context) (*Session, error)
	Do(ctx context.Context, retryNoIdempotent bool, op RetryOperation) (err error)
	Close(ctx context.Context) error
}
