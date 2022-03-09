package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
)

// WithTraceID returns a copy of parent context with traceID
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return meta.WithTraceID(ctx, traceID)
}

// WithRequestType returns a copy of parent context with custom request type
func WithRequestType(ctx context.Context, requestType string) context.Context {
	return meta.WithRequestType(ctx, requestType)
}
