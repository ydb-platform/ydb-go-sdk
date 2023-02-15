package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
)

// WithTraceID returns a copy of parent context with traceID
//
// Deprecated: use meta.WithTraceID instead
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return meta.WithTraceID(ctx, traceID)
}

// WithRequestType returns a copy of parent context with custom request type
//
// Deprecated: use meta.WithRequestType instead
func WithRequestType(ctx context.Context, requestType string) context.Context {
	return meta.WithRequestType(ctx, requestType)
}
