package balancer

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

type (
	ctxEndpointKey struct{}
	ctxCancelGuard struct{}
)

type Endpoint interface {
	NodeID() uint32
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

func childCloser(ctx context.Context) *xcontext.CancelsGuard {
	if g, ok := ctx.Value(ctxCancelGuard{}).(*xcontext.CancelsGuard); ok {
		return g
	}

	return nil
}

func OnDropConn(ctx context.Context, closeChild func()) context.Context {
	g := childCloser(ctx)
	if g == nil {
		g = xcontext.NewCancelsGuard()
		ctx = context.WithValue(ctx, ctxCancelGuard{}, g)
	}
	cancel := context.CancelFunc(func() {
		closeChild()
	})
	g.Remember(&cancel)
	return ctx
}
