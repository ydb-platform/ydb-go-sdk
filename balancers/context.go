package balancers

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

// WithNodeID returns the copy of context with NodeID which the client balancer will
// prefer on step of choose YDB endpoint step.
//
// When the balancer MaxConnections limit is enabled and the pinned node is
// outside the active set, the balancer may soft-exceed the limit and open a
// connection to that node (see [WithMaxConnections]).
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithNodeID(ctx context.Context, nodeID uint32) context.Context {
	return endpoint.WithNodeID(ctx, nodeID)
}
