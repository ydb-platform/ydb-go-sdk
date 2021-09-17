package ydb

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
)

type DB interface {
	grpc.ClientConnInterface

	Table() table.Client
	Scheme() scheme.Client
}

type db struct {
	database string
	options  options
	cluster  cluster.Cluster
	table    *lazyTable
	scheme   *lazyScheme
}

func (c *db) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	return c.cluster.Invoke(ctx, method, args, reply, opts...)
}

func (c *db) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.cluster.NewStream(ctx, desc, method, opts...)
}

func (c *db) Stats(it func(cluster.Endpoint, stats.Stats)) {
	c.cluster.Stats(it)
}

func (c *db) Close() error {
	_ = c.Table().Close(context.Background())
	_ = c.Scheme().Close(context.Background())
	return c.cluster.Close()
}

func (c *db) Table() table.Client {
	return c.table
}

func (c *db) Scheme() scheme.Client {
	return c.scheme
}
