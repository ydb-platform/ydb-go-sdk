package driver

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
)

type (
	ctxClientConnApplierKey struct{}
)

type ClientConnApplier func(c cluster.ClientConnInterface)

// WithClientConnApplier returns a copy of parent context with client Conn applier function
func WithClientConnApplier(ctx context.Context, apply ClientConnApplier) context.Context {
	if exist, ok := ContextClientConnApplier(ctx); ok {
		return context.WithValue(
			ctx,
			ctxClientConnApplierKey{},
			ClientConnApplier(func(conn cluster.ClientConnInterface) {
				exist(conn)
				apply(conn)
			}),
		)
	}
	return context.WithValue(ctx, ctxClientConnApplierKey{}, apply)
}

// ContextClientConnApplier returns the ClientConnApplier within given context.
func ContextClientConnApplier(ctx context.Context) (v ClientConnApplier, ok bool) {
	v, ok = ctx.Value(ctxClientConnApplierKey{}).(ClientConnApplier)
	return
}
