package endpoint

import "context"

type (
	ctxEndpointKey  struct{}
	ctxPinnedNodeID uint32

	withNodeIDConfig struct {
		disableFallback bool
	}

	// WithNodeIDOption configures [WithNodeID].
	WithNodeIDOption func(*withNodeIDConfig)
)

// WithDisableFallback disables fallback to another endpoint when the preferred node is unavailable.
func WithDisableFallback() WithNodeIDOption {
	return func(cfg *withNodeIDConfig) {
		cfg.disableFallback = true
	}
}

func WithNodeID(ctx context.Context, nodeID uint32, opts ...WithNodeIDOption) context.Context {
	var cfg withNodeIDConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.disableFallback {
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

// ContextDisableFallback reports whether [WithNodeID] was called with [WithDisableFallback].
func ContextDisableFallback(ctx context.Context) bool {
	_, ok := ctx.Value(ctxEndpointKey{}).(ctxPinnedNodeID)

	return ok
}
