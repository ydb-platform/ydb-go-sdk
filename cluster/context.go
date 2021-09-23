package cluster

import (
	"context"
)

type (
	ctxClientConnApplierKey struct{}
)

type ClientConnApplier func(c ClientConnInterface)

// WithClientConnApplier returns a copy of parent deadline with client Conn applier function
func WithClientConnApplier(ctx context.Context, apply ClientConnApplier) context.Context {
	if exist, ok := ContextClientConnApplier(ctx); ok {
		return context.WithValue(
			ctx,
			ctxClientConnApplierKey{},
			ClientConnApplier(func(conn ClientConnInterface) {
				exist(conn)
				apply(conn)
			}),
		)
	}
	return context.WithValue(ctx, ctxClientConnApplierKey{}, apply)
}

// ContextClientConnApplier returns the ClientConnApplier within given deadline.
func ContextClientConnApplier(ctx context.Context) (v ClientConnApplier, ok bool) {
	v, ok = ctx.Value(ctxClientConnApplierKey{}).(ClientConnApplier)
	return
}
