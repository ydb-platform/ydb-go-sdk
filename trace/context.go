package trace

import "context"

type driverTraceContextKey struct{}

// WithDriverTrace returns context which has associated DriverTrace with it.
func WithDriverTrace(ctx context.Context, t DriverTrace) context.Context {
	return context.WithValue(ctx,
		driverTraceContextKey{},
		ContextDriverTrace(ctx).Compose(t),
	)
}

// ContextDriverTrace returns DriverTrace associated with ctx.
// If there is no DriverTrace associated with ctx then zero value
// of DriverTrace is returned.
func ContextDriverTrace(ctx context.Context) DriverTrace {
	t, _ := ctx.Value(driverTraceContextKey{}).(DriverTrace)
	return t
}

type retryTraceContextKey struct{}

// WithRetryTrace returns context which has associated RetryTrace with it.
func WithRetryTrace(ctx context.Context, t RetryTrace) context.Context {
	return context.WithValue(ctx,
		retryTraceContextKey{},
		ContextRetryTrace(ctx).Compose(t),
	)
}

// ContextRetryTrace returns RetryTrace associated with ctx.
// If there is no RetryTrace associated with ctx then zero value
// of RetryTrace is returned.
func ContextRetryTrace(ctx context.Context) RetryTrace {
	t, _ := ctx.Value(retryTraceContextKey{}).(RetryTrace)
	return t
}
