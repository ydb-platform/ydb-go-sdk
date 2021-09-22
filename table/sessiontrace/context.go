package sessiontrace

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
)

type clientTraceContextKey struct{}

// WithClientTrace returns context which has associated Trace with it.
func WithClientTrace(ctx context.Context, t table.Trace) context.Context {
	return context.WithValue(ctx,
		clientTraceContextKey{},
		ContextClientTrace(ctx).Compose(t),
	)
}

// ContextClientTrace returns Trace associated with ctx.
// If there is no Trace associated with ctx then zero value
// of Trace is returned.
func ContextClientTrace(ctx context.Context) table.Trace {
	t, _ := ctx.Value(clientTraceContextKey{}).(table.Trace)
	return t
}

type sessionPoolTraceContextKey struct{}

// WithSessionPoolTrace returns context which has associated SessionPoolTrace with it.
func WithSessionPoolTrace(ctx context.Context, t table.SessionPoolTrace) context.Context {
	return context.WithValue(ctx,
		sessionPoolTraceContextKey{},
		ContextSessionPoolTrace(ctx).Compose(t),
	)
}

// ContextSessionPoolTrace returns SessionPoolTrace associated with ctx.
// If there is no SessionPoolTrace associated with ctx then zero value
// of SessionPoolTrace is returned.
func ContextSessionPoolTrace(ctx context.Context) table.SessionPoolTrace {
	t, _ := ctx.Value(sessionPoolTraceContextKey{}).(table.SessionPoolTrace)
	return t
}
