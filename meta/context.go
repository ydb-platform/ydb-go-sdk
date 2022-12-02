package meta

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
)

// WithTraceID returns a copy of parent context with traceID
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, meta.HeaderTraceID, traceID)
}

// WithUserAgent returns a copy of parent context with custom user-agent info
func WithUserAgent(ctx context.Context, userAgent string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, meta.HeaderUserAgent, userAgent)
}

// WithRequestType returns a copy of parent context with custom request type
func WithRequestType(ctx context.Context, requestType string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, meta.HeaderRequestType, requestType)
}

// WithAllowFeatures returns a copy of parent context with allowed client feature
func WithAllowFeatures(ctx context.Context, features ...string) context.Context {
	kv := make([]string, 0, len(features)*2)
	for _, feature := range features {
		kv = append(kv, meta.HeaderClientCapabilities, feature)
	}
	return metadata.AppendToOutgoingContext(ctx, kv...)
}

// WithTrailerCallback attaches callback to context for listening incoming metadata
func WithTrailerCallback(
	ctx context.Context,
	callback func(md metadata.MD),
) context.Context {
	return meta.WithTrailerCallback(ctx, callback)
}
