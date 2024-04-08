package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
)

// WithTraceID returns a copy of parent context with traceID
//
// Deprecated: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return meta.WithTraceID(ctx, traceID)
}

// WithRequestType returns a copy of parent context with custom request type
//
// Deprecated: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithRequestType(ctx context.Context, requestType string) context.Context {
	return meta.WithRequestType(ctx, requestType)
}
