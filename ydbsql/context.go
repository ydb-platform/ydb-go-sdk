package ydbsql

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type ctxScanQueryKey struct{}

// WithScanQuery returns a copy of parent context with scan query flag.
func WithScanQuery(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxScanQueryKey{}, struct{}{})
}

// ContextScanQueryMode returns true if context contains scan query flag.
func ContextScanQueryMode(ctx context.Context) bool {
	return ctx.Value(ctxScanQueryKey{}) != nil
}

type ctxConfigKey struct{}

// WithTableConfig returns a copy of parent context with table config.
func WithTableConfig(ctx context.Context, config table.Config) context.Context {
	return context.WithValue(ctx, ctxConfigKey{}, &config)
}

// ContextTableConfig returns table config from context.
func ContextTableConfig(ctx context.Context) table.Config {
	if cfg, ok := ctx.Value(ctxConfigKey{}).(*table.Config); ok {
		return *cfg
	}
	return table.DefaultConfig()
}
