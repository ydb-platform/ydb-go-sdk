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

// WithRetry returns context which has associated Retry with it.
func WithRetry(ctx context.Context, t Retry) context.Context {
	return context.WithValue(ctx,
		retryContextKey{},
		ContextRetry(ctx).Compose(t),
	)
}

// ContextRetry returns Retry associated with ctx.
// If there is no Retry associated with ctx then zero value
// of Retry is returned.
func ContextRetry(ctx context.Context) Retry {
	t, _ := ctx.Value(retryContextKey{}).(Retry)
	return t
}

type clientTraceContextKey struct{}

// WithClientTrace returns context which has associated Trace with it.
func WithClientTrace(ctx context.Context, t Table) context.Context {
	return context.WithValue(ctx,
		clientTraceContextKey{},
		ContextTable(ctx).Compose(t),
	)
}

// ContextTable returns Table associated with ctx.
// If there is no Table associated with ctx then zero value
// of Trace is returned.
func ContextTable(ctx context.Context) Table {
	t, _ := ctx.Value(clientTraceContextKey{}).(Table)
	return t
}
