package sessiontrace

import "context"

type clientTraceContextKey struct{}

// WithClientTrace returns context which has associated Trace with it.
func WithClientTrace(ctx context.Context, t Trace) context.Context {
	return context.WithValue(ctx,
		clientTraceContextKey{},
		ContextClientTrace(ctx).Compose(t),
	)
}

// ContextClientTrace returns Trace associated with ctx.
// If there is no Trace associated with ctx then zero value
// of Trace is returned.
func ContextClientTrace(ctx context.Context) Trace {
	t, _ := ctx.Value(clientTraceContextKey{}).(Trace)
	return t
}

type createSessionTraceContextKey struct{}

// withCreateSessionTrace returns context which has associated createSessionTrace with it.
func withCreateSessionTrace(ctx context.Context, t createSessionTrace) context.Context {
	return context.WithValue(ctx,
		createSessionTraceContextKey{},
		contextCreateSessionTrace(ctx).Compose(t),
	)
}

// contextCreateSessionTrace returns createSessionTrace associated with ctx.
// If there is no createSessionTrace associated with ctx then zero value
// of createSessionTrace is returned.
func contextCreateSessionTrace(ctx context.Context) createSessionTrace {
	t, _ := ctx.Value(createSessionTraceContextKey{}).(createSessionTrace)
	return t
}

type sessionPoolTraceContextKey struct{}

// WithSessionPoolTrace returns context which has associated SessionPoolTrace with it.
func WithSessionPoolTrace(ctx context.Context, t SessionPoolTrace) context.Context {
	return context.WithValue(ctx,
		sessionPoolTraceContextKey{},
		ContextSessionPoolTrace(ctx).Compose(t),
	)
}

// ContextSessionPoolTrace returns SessionPoolTrace associated with ctx.
// If there is no SessionPoolTrace associated with ctx then zero value
// of SessionPoolTrace is returned.
func ContextSessionPoolTrace(ctx context.Context) SessionPoolTrace {
	t, _ := ctx.Value(sessionPoolTraceContextKey{}).(SessionPoolTrace)
	return t
}
