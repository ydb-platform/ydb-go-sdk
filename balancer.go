package ydb

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type balancerWithMeta struct {
	balancer *balancer.Balancer
	meta     *meta.Meta
}

func newBalancerWithMeta(b *balancer.Balancer, m *meta.Meta) *balancerWithMeta {
	return &balancerWithMeta{balancer: b, meta: m}
}

func (b *balancerWithMeta) Invoke(ctx context.Context, method string, args any, reply any,
	opts ...grpc.CallOption,
) error {
	metaCtx, err := b.meta.Context(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return b.balancer.Invoke(metaCtx, method, args, reply, opts...)
}

func (b *balancerWithMeta) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	metaCtx, err := b.meta.Context(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return b.balancer.NewStream(metaCtx, desc, method, opts...)
}

func (b *balancerWithMeta) Close(ctx context.Context) error {
	return b.balancer.Close(ctx)
}
