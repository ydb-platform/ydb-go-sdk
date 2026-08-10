package balancers

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

// WithNodeID returns a copy of context which makes the client balancer select
// the requested node before applying its endpoint-selection policies.
//
// When a maximum connection limit is configured and the node is outside the
// active set, the balancer may open an additional connection beyond the limit.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithNodeID(ctx context.Context, nodeID uint32) context.Context {
	return endpoint.WithNodeID(ctx, nodeID)
}
