package conn

import "context"

type ctxNoWrappingKey struct{}

func WithoutWrapping(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoWrappingKey{}, true)
}

func UseWrapping(ctx context.Context) bool {
	b, ok := ctx.Value(ctxNoWrappingKey{}).(bool)

	return !ok || !b
}

type ctxPessimizeOnOverloadedKey struct{}

// WithPessimizeOnOverloaded marks the context so that the balancer will pessimize the
// connection when an OVERLOADED operation error is received. This should be used for
// session creation requests, where OVERLOADED means the node has reached its session limit.
func WithPessimizeOnOverloaded(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxPessimizeOnOverloadedKey{}, true)
}

// IsPessimizeOnOverloaded reports whether the context is marked to pessimize the
// connection when an OVERLOADED operation error is received.
func IsPessimizeOnOverloaded(ctx context.Context) bool {
	b, ok := ctx.Value(ctxPessimizeOnOverloadedKey{}).(bool)

	return ok && b
}
