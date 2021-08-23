package ydb

import (
	"context"
	"google.golang.org/grpc"
)

type Cluster interface {
	Get(ctx context.Context) (grpcConn grpc.ClientConnInterface, err error)
	Close() error
}
