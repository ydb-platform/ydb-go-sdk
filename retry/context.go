package retry

import "context"

type (
	ctxIsOperationIdempotentKey struct{}
)

// WithIdempotentOperation returns a copy of parent context with idempotent operation feature
//
// Deprecated: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#deprecated
func WithIdempotentOperation(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxIsOperationIdempotentKey{}, true)
}

// WithNonIdempotentOperation returns a copy of parent context with non-idempotent operation feature
//
// Deprecated: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#deprecated
func WithNonIdempotentOperation(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxIsOperationIdempotentKey{}, false)
}

// IsOperationIdempotent returns the flag for retry with no idempotent errors
//
// Deprecated: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#deprecated
func IsOperationIdempotent(ctx context.Context) bool {
	v, ok := ctx.Value(ctxIsOperationIdempotentKey{}).(bool)

	return ok && v
}
