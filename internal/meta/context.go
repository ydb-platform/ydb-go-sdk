package meta

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// WithTraceID returns a copy of parent deadline with traceID
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, HeaderTraceID, traceID)
}

// WithUserAgent returns a copy of parent deadline with custom user-agent info
func WithUserAgent(ctx context.Context, userAgent string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, HeaderUserAgent, userAgent)
}
