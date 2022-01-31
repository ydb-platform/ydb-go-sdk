package proxy

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
)

type proxyRatelimiter struct {
	client ydb_ratelimiter.Client
	meta   meta.Meta
}

func Ratelimiter(client ydb_ratelimiter.Client, meta meta.Meta) *proxyRatelimiter {
	return &proxyRatelimiter{
		client: client,
		meta:   meta,
	}
}

func (r *proxyRatelimiter) Close(ctx context.Context) (err error) {
	ctx, err = r.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return r.client.Close(ctx)
}

func (r *proxyRatelimiter) CreateResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ydb_ratelimiter.Resource,
) (err error) {
	ctx, err = r.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return r.client.CreateResource(ctx, coordinationNodePath, resource)
}

func (r *proxyRatelimiter) AlterResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ydb_ratelimiter.Resource,
) (err error) {
	ctx, err = r.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return r.client.AlterResource(ctx, coordinationNodePath, resource)
}

func (r *proxyRatelimiter) DropResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (err error) {
	ctx, err = r.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return r.client.DropResource(ctx, coordinationNodePath, resourcePath)
}

func (r *proxyRatelimiter) ListResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	recursive bool,
) (_ []string, err error) {
	ctx, err = r.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return r.client.ListResource(ctx, coordinationNodePath, resourcePath, recursive)
}

func (r *proxyRatelimiter) DescribeResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (_ *ydb_ratelimiter.Resource, err error) {
	ctx, err = r.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return r.client.DescribeResource(ctx, coordinationNodePath, resourcePath)
}

func (r *proxyRatelimiter) AcquireResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	amount uint64,
	isUsedAmount bool,
) (err error) {
	ctx, err = r.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return r.client.AcquireResource(ctx, coordinationNodePath, resourcePath, amount, isUsedAmount)
}
