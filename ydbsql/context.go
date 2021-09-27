package ydbsql

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
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

type ctxConfigKey struct{}

// WithTableConfig returns a copy of parent deadline with table config.
func WithTableConfig(ctx context.Context, config table.Config) context.Context {
	return context.WithValue(ctx, ctxConfigKey{}, &config)
}

// ContextTableConfig returns table config from deadline.
func ContextTableConfig(ctx context.Context) table.Config {
	if cfg, ok := ctx.Value(ctxConfigKey{}).(*table.Config); ok {
		return *cfg
	}
	return table.DefaultConfig()
}

type ctxRetryNoIdempotentKey struct{}

// WithRetryNoIdempotent returns a copy of parent context with allow operationCompleted
// operations with no idempotent errors
func WithRetryNoIdempotent(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxRetryNoIdempotentKey{}, true)
}

// ContextRetryNoIdempotent returns the flag for operationCompleted with no idempotent errors
func ContextRetryNoIdempotent(ctx context.Context) bool {
	v, ok := ctx.Value(ctxRetryNoIdempotentKey{}).(bool)
	return ok && v
}
