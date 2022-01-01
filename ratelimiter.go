package ydb

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
)

type lazyRatelimiter struct {
	db     DB
	client internal.Client
	m      sync.Mutex
}

func (r *lazyRatelimiter) Close(ctx context.Context) error {
	r.m.Lock()
	defer r.m.Unlock()
	if r.client == nil {
		return nil
	}
	defer func() {
		r.client = nil
	}()
	return r.client.Close(ctx)
}

func (r *lazyRatelimiter) CreateResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	r.init()
	return r.client.CreateResource(ctx, coordinationNodePath, resource)
}

func (r *lazyRatelimiter) AlterResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	r.init()
	return r.client.AlterResource(ctx, coordinationNodePath, resource)
}

func (r *lazyRatelimiter) DropResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (err error) {
	r.init()
	return r.client.DropResource(ctx, coordinationNodePath, resourcePath)
}

func (r *lazyRatelimiter) ListResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	recursive bool,
) (_ []string, err error) {
	r.init()
	return r.client.ListResource(ctx, coordinationNodePath, resourcePath, recursive)
}

func (r *lazyRatelimiter) DescribeResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (_ *ratelimiter.Resource, err error) {
	r.init()
	return r.client.DescribeResource(ctx, coordinationNodePath, resourcePath)
}

func (r *lazyRatelimiter) AcquireResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	amount uint64,
	isUsedAmount bool,
) (err error) {
	r.init()
	return r.client.AcquireResource(ctx, coordinationNodePath, resourcePath, amount, isUsedAmount)
}

func (r *lazyRatelimiter) init() {
	r.m.Lock()
	if r.client == nil {
		r.client = internal.New(r.db)
	}
	r.m.Unlock()
}
