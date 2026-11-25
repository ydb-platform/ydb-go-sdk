package conn

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

type fallbackCodecKey struct{}

// WithFallbackCodec requests a per-call codec when no codec was configured.
// Transports without [FallbackCodecStreamInterceptor] retain their normal codec.
func WithFallbackCodec(ctx context.Context, codec encoding.CodecV2) context.Context {
	return context.WithValue(ctx, fallbackCodecKey{}, codec)
}

// FallbackCodecStreamInterceptor selects a fallback after gRPC combines connection
// defaults and call options. Install it after other stream interceptors.
func FallbackCodecStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if codec, ok := ctx.Value(fallbackCodecKey{}).(encoding.CodecV2); ok && !HasCodecCallOption(opts...) {
		fallbackOpts := make([]grpc.CallOption, len(opts)+1)
		copy(fallbackOpts, opts)
		fallbackOpts[len(opts)] = grpc.ForceCodecV2(codec)
		opts = fallbackOpts
	}

	return streamer(ctx, desc, cc, method, opts...)
}

// HasCodecCallOption reports an explicit codec or content subtype selection.
func HasCodecCallOption(opts ...grpc.CallOption) bool {
	for _, opt := range opts {
		switch opt.(type) {
		case grpc.ForceCodecCallOption, *grpc.ForceCodecCallOption,
			grpc.ForceCodecV2CallOption, *grpc.ForceCodecV2CallOption,
			grpc.CustomCodecCallOption, *grpc.CustomCodecCallOption,
			grpc.ContentSubtypeCallOption, *grpc.ContentSubtypeCallOption:
			return true
		}
	}

	return false
}
