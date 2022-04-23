package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/database"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

type lazyRatelimiter struct {
	db     database.Connection
	config config.Config
	c      ratelimiter.Client
	m      sync.Mutex
}

func Ratelimiter(db database.Connection, options []config.Option) ratelimiter.Client {
	return &lazyRatelimiter{
		db:     db,
		config: config.New(options...),
	}
}

func (r *lazyRatelimiter) Close(ctx context.Context) (err error) {
	r.m.Lock()
	defer r.m.Unlock()
	if r.c == nil {
		return nil
	}
	return r.c.Close(ctx)
}

func (r *lazyRatelimiter) CreateResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	if !r.config.AutoRetry() {
		return r.client().CreateResource(ctx, coordinationNodePath, resource)
	}
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return r.client().CreateResource(ctx, coordinationNodePath, resource)
	})
}

func (r *lazyRatelimiter) AlterResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	if !r.config.AutoRetry() {
		return r.client().AlterResource(ctx, coordinationNodePath, resource)
	}
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return r.client().AlterResource(ctx, coordinationNodePath, resource)
	})
}

func (r *lazyRatelimiter) DropResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (err error) {
	if !r.config.AutoRetry() {
		return r.client().DropResource(ctx, coordinationNodePath, resourcePath)
	}
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return r.client().DropResource(ctx, coordinationNodePath, resourcePath)
	})
}

func (r *lazyRatelimiter) ListResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	recursive bool,
) (paths []string, err error) {
	if !r.config.AutoRetry() {
		return r.client().ListResource(ctx, coordinationNodePath, resourcePath, recursive)
	}
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		paths, err = r.client().ListResource(ctx, coordinationNodePath, resourcePath, recursive)
		return err
	})
	return paths, err
}

func (r *lazyRatelimiter) DescribeResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (resource *ratelimiter.Resource, err error) {
	if !r.config.AutoRetry() {
		return r.client().DescribeResource(ctx, coordinationNodePath, resourcePath)
	}
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		resource, err = r.client().DescribeResource(ctx, coordinationNodePath, resourcePath)
		return err
	})
	return resource, err
}

func (r *lazyRatelimiter) AcquireResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	amount uint64,
	opts ...options.AcquireOption,
) (err error) {
	if !r.config.AutoRetry() {
		return r.client().AcquireResource(ctx, coordinationNodePath, resourcePath, amount, opts...)
	}
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return r.client().AcquireResource(ctx, coordinationNodePath, resourcePath, amount, opts...)
	})
}

func (r *lazyRatelimiter) client() ratelimiter.Client {
	r.m.Lock()
	defer r.m.Unlock()
	if r.c == nil {
		r.c = builder.New(r.db, r.config)
	}
	return r.c
}
