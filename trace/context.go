package trace

import "context"

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
