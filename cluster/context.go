package cluster

import (
	"context"
)

type (
	ctxNodeIDKey struct{}
	NodeID       uint32
)

func WithNodeID(ctx context.Context, nodeID NodeID) context.Context {
	return context.WithValue(ctx, ctxNodeIDKey{}, nodeID)
}

func ContextNodeID(ctx context.Context) (nodeID NodeID, ok bool) {
	nodeID, ok = ctx.Value(ctxNodeIDKey{}).(NodeID)
	return nodeID, ok
}
