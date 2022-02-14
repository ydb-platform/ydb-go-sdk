package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

type lazyRatelimiter struct {
	db      db.Connection
	options []config.Option
	client  ratelimiter.Client
	m       sync.Mutex
}

func Ratelimiter(db db.Connection, options []config.Option) ratelimiter.Client {
	return &lazyRatelimiter{
		db:      db,
		options: options,
	}
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
	return retry.Retry(ctx, false, func(ctx context.Context) (err error) {
		return r.client.CreateResource(ctx, coordinationNodePath, resource)
	})
}

func (r *lazyRatelimiter) AlterResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	r.init()
	return retry.Retry(ctx, false, func(ctx context.Context) (err error) {
		return r.client.AlterResource(ctx, coordinationNodePath, resource)
	})
}

func (r *lazyRatelimiter) DropResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (err error) {
	r.init()
	return retry.Retry(ctx, false, func(ctx context.Context) (err error) {
		return r.client.DropResource(ctx, coordinationNodePath, resourcePath)
	})
}

func (r *lazyRatelimiter) ListResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	recursive bool,
) (paths []string, err error) {
	r.init()
	err = retry.Retry(ctx, false, func(ctx context.Context) (err error) {
		paths, err = r.client.ListResource(ctx, coordinationNodePath, resourcePath, recursive)
		return err
	})
	return paths, err
}

func (r *lazyRatelimiter) DescribeResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (resource *ratelimiter.Resource, err error) {
	r.init()
	err = retry.Retry(ctx, false, func(ctx context.Context) (err error) {
		resource, err = r.client.DescribeResource(ctx, coordinationNodePath, resourcePath)
		return err
	})
	return resource, err
}

func (r *lazyRatelimiter) AcquireResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	amount uint64,
	isUsedAmount bool,
) (err error) {
	r.init()
	return retry.Retry(ctx, false, func(ctx context.Context) (err error) {
		return r.client.AcquireResource(ctx, coordinationNodePath, resourcePath, amount, isUsedAmount)
	})
}

func (r *lazyRatelimiter) init() {
	r.m.Lock()
	if r.client == nil {
		r.client = builder.New(r.db, r.options)
	}
	r.m.Unlock()
}
