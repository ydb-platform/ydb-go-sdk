package endpoint

import "context"

type (
	ctxEndpointKey  struct{}
	ctxPinnedNodeID uint32

	withNodeIDConfig struct {
		fallback bool
	}

	// WithNodeIDOption configures [WithNodeID].
	WithNodeIDOption func(*withNodeIDConfig)
)

// WithFallback controls whether the balancer may use another endpoint when the preferred node
// is unavailable. Default is true when the option is omitted.
func WithFallback(enabled bool) WithNodeIDOption {
	return func(cfg *withNodeIDConfig) {
		cfg.fallback = enabled
	}
}

func WithNodeID(ctx context.Context, nodeID uint32, opts ...WithNodeIDOption) context.Context {
	cfg := withNodeIDConfig{
		fallback: true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if !cfg.fallback {
		return context.WithValue(ctx, ctxEndpointKey{}, ctxPinnedNodeID(nodeID))
	}

	return context.WithValue(ctx, ctxEndpointKey{}, nodeID)
}

func ContextNodeID(ctx context.Context) (nodeID uint32, ok bool) {
	switch v := ctx.Value(ctxEndpointKey{}).(type) {
	case uint32:
		return v, true
	case ctxPinnedNodeID:
		return uint32(v), true
	default:
		return 0, false
	}
}

// ContextFallback reports whether endpoint fallback is allowed for [WithNodeID] contexts.
// Returns true by default, including when no node preference is set in the context.
func ContextFallback(ctx context.Context) bool {
	_, pinned := ctx.Value(ctxEndpointKey{}).(ctxPinnedNodeID)

	return !pinned
}
