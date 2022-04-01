package trace

import "context"

type retryContextKey struct{}

// WithRetry returns deadline which has associated Retry with it.
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

type tableContextKey struct{}

// WithTable returns deadline which has associated Trace with it.
func WithTable(ctx context.Context, t Table) context.Context {
	return context.WithValue(ctx,
		tableContextKey{},
		ContextTable(ctx).Compose(t),
	)
}

// ContextTable returns Table associated with ctx.
// If there is no Table associated with ctx then zero value
// of Trace is returned.
func ContextTable(ctx context.Context) Table {
	t, _ := ctx.Value(tableContextKey{}).(Table)
	return t
}
