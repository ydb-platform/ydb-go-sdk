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

type nodeIDer struct {
	nodeID uint32
}

func (n nodeIDer) NodeID() uint32 {
	return n.nodeID
}

func WithNodeID(ctx context.Context, nodeID uint32) context.Context {
	return WithEndpoint(ctx, nodeIDer{nodeID: nodeID})
}

func ContextEndpoint(ctx context.Context) (e Endpoint, ok bool) {
	if e, ok = ctx.Value(ctxEndpointKey{}).(Endpoint); ok {
		return e, true
	}
	return nil, false
}
