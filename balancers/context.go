package balancers

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func WithNodeID(ctx context.Context, nodeID uint32) context.Context {
	return endpoint.WithNodeID(ctx, nodeID)
}
