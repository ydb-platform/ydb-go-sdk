package retry

import "context"

type (
	ctxIsOperationIdempotentKey struct{}
)

// WithIdempotentOperation returns a copy of parent context with idempotent
// operation feature
func WithIdempotentOperation(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxIsOperationIdempotentKey{}, true)
}

// WithNonIdempotentOperation returns a copy of parent context with non-idempotent
// operation feature
func WithNonIdempotentOperation(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxIsOperationIdempotentKey{}, false)
}

// IsOperationIdempotent returns the flag for retry with no idempotent errors
func IsOperationIdempotent(ctx context.Context) bool {
	v, ok := ctx.Value(ctxIsOperationIdempotentKey{}).(bool)
	return ok && v
}
