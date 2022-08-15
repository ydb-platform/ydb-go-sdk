package balancer

import "context"

type (
	ctxEndpointKey struct{}
)

type Endpoint interface {
	NodeID() uint32
}

func WithEndpoint(ctx context.Context, endpoint Endpoint) context.Context {
	return context.WithValue(ctx, ctxEndpointKey{}, endpoint)
}

type nodeID uint32

func (n nodeID) NodeID() uint32 {
	return uint32(n)
}

func WithNodeID(ctx context.Context, id uint32) context.Context {
	return WithEndpoint(ctx, nodeID(id))
}

func ContextEndpoint(ctx context.Context) (e Endpoint, ok bool) {
	if e, ok = ctx.Value(ctxEndpointKey{}).(Endpoint); ok {
		return e, true
	}
	return nil, false
}
