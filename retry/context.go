package retry

import "context"

type (
	ctxRetryNoIdempotentKey struct{}
)

// WithRetryNoIdempotent returns a copy of parent context with allow retry
// operations with no idempotent errors
func WithRetryNoIdempotent(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxRetryNoIdempotentKey{}, true)
}

// ContextRetryNoIdempotent returns the flag for retry with no idempotent errors
func ContextRetryNoIdempotent(ctx context.Context) bool {
	v, ok := ctx.Value(ctxRetryNoIdempotentKey{}).(bool)
	return ok && v
}
