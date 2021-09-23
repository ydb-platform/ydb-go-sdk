package trace

import "context"

type driverContextKey struct{}

// WithDriver returns context which has associated Driver with it.
func WithDriver(ctx context.Context, t Driver) context.Context {
	return context.WithValue(ctx,
		driverContextKey{},
		ContextDriver(ctx).Compose(t),
	)
}

// ContextDriver returns Driver associated with ctx.
// If there is no Driver associated with ctx then zero value
// of Driver is returned.
func ContextDriver(ctx context.Context) Driver {
	t, _ := ctx.Value(driverContextKey{}).(Driver)
	return t
}

type retryContextKey struct{}

// WithRetry returns context which has associated RetryTrace with it.
func WithRetry(ctx context.Context, t RetryTrace) context.Context {
	return context.WithValue(ctx,
		retryContextKey{},
		ContextRetry(ctx).Compose(t),
	)
}

// ContextRetry returns RetryTrace associated with ctx.
// If there is no RetryTrace associated with ctx then zero value
// of RetryTrace is returned.
func ContextRetry(ctx context.Context) RetryTrace {
	t, _ := ctx.Value(retryContextKey{}).(RetryTrace)
	return t
}
