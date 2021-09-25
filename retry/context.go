package retry

import "context"

type (
	ctxIdempotentOperationKey struct{}
)

// WithIdempotentOperation returns a copy of parent context with idempotent
// operation feature
func WithIdempotentOperation(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxIdempotentOperationKey{}, true)
}

// WithNonIdempotentOperation returns a copy of parent context with non-idempotent
// operation feature
func WithNonIdempotentOperation(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxIdempotentOperationKey{}, false)
}

// ContextIdempotentOperation returns the flag for retry with no idempotent errors
func ContextIdempotentOperation(ctx context.Context) bool {
	v, ok := ctx.Value(ctxIdempotentOperationKey{}).(bool)
	return ok && v
}
