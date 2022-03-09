package meta

import (
	"context"

	"google.golang.org/grpc/metadata"
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
