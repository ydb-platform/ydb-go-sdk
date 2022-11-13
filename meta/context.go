package meta

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/incoming"
)

// WithTraceID returns a copy of parent context with traceID
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, HeaderTraceID, traceID)
}

// WithUserAgent returns a copy of parent context with custom user-agent info
func WithUserAgent(ctx context.Context, userAgent string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, HeaderUserAgent, userAgent)
}

// WithRequestType returns a copy of parent context with custom request type
func WithRequestType(ctx context.Context, requestType string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, HeaderRequestType, requestType)
}

// WithAllowFeatures returns a copy of parent context with allowed client feature
func WithAllowFeatures(ctx context.Context, features ...string) context.Context {
	kv := make([]string, 0, len(features)*2)
	for _, feature := range features {
		kv = append(kv, HeaderClientCapabilities, feature)
	}
	return metadata.AppendToOutgoingContext(ctx, kv...)
}

func WithIncomingMetadataCallback(ctx context.Context, callback func(header string, values []string)) context.Context {
	return incoming.WithMetadataCallback(ctx, callback)
}
