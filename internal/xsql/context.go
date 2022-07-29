package xsql

import "context"

type ctxModeTypeKey struct{}

// WithQueryMode returns a copy of parent context with given QueryMode
func WithQueryMode(ctx context.Context, m QueryMode) context.Context {
	return context.WithValue(ctx, ctxModeTypeKey{}, m)
}

// QueryModeFromContext returns defined QueryMode or QueryModeDefault
func QueryModeFromContext(ctx context.Context) QueryMode {
	if m, ok := ctx.Value(ctxModeTypeKey{}).(QueryMode); ok {
		return m
	}
	return QueryModeDefault
}
