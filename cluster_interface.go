package ydb

import (
	"context"
	"google.golang.org/grpc"
)

type ClientConnInterface interface {
	grpc.ClientConnInterface

	Address() string
}

type Cluster interface {
	Get(ctx context.Context) (conn ClientConnInterface, err error)
	Close() error
}
