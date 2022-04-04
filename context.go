package ydb

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
)

// WithOperationTimeout returns a copy of parent context in which YDB operation timeout
// parameter is set to d. If parent context timeout is smaller than d, parent context is returned.
func WithOperationTimeout(ctx context.Context, operationTimeout time.Duration) context.Context {
	return operation.WithTimeout(ctx, operationTimeout)
}

// WithOperationCancelAfter returns a copy of parent context in which YDB operation
// cancel after parameter is set to d. If parent context cancellation timeout is smaller
// than d, parent context is returned.
func WithOperationCancelAfter(ctx context.Context, operationCancelAfter time.Duration) context.Context {
	return operation.WithCancelAfter(ctx, operationCancelAfter)
}
