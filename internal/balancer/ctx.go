package balancer

import "context"

type (
	ctxEndpointKey struct{}
)

type Endpoint interface {
	NodeID() uint32

	// Choose calls from balancer if this endpoint to request chosen or not
	Choose(chosen bool)
}

func WithEndpoint(ctx context.Context, endpoint Endpoint) context.Context {
	return context.WithValue(ctx, ctxEndpointKey{}, endpoint)
}

func ContextEndpoint(ctx context.Context) (e Endpoint, ok bool) {
	if e, ok = ctx.Value(ctxEndpointKey{}).(Endpoint); ok {
		return e, true
	}
	return nil, false
}
