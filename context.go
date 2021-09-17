package ydb

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
)

type ClientConnApplier cluster.ClientConnApplier

func WithClientConnApplier(ctx context.Context, apply ClientConnApplier) context.Context {
	return cluster.WithClientConnApplier(ctx, cluster.ClientConnApplier(apply))
}
