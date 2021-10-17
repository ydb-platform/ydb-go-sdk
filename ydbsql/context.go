package ydbsql

import (
	"context"
)

type ctxScanQueryKey struct{}

// WithScanQuery returns a copy of parent deadline with scan query flag.
func WithScanQuery(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxScanQueryKey{}, struct{}{})
}

// ContextScanQueryMode returns true if deadline contains scan query flag.
func ContextScanQueryMode(ctx context.Context) bool {
	return ctx.Value(ctxScanQueryKey{}) != nil
}
