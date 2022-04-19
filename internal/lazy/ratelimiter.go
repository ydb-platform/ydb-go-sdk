package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/database"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

type lazyRatelimiter struct {
	db      database.Connection
	options []config.Option
	c       ratelimiter.Client
	m       sync.Mutex
}

func Ratelimiter(db database.Connection, options []config.Option) ratelimiter.Client {
	return &lazyRatelimiter{
		db:      db,
		options: options,
	}
}

func (r *lazyRatelimiter) Close(ctx context.Context) (err error) {
	r.m.Lock()
	defer r.m.Unlock()
	if r.c == nil {
		return nil
	}
	defer func() {
		r.c = nil
	}()
	err = r.c.Close(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

func (r *lazyRatelimiter) CreateResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return r.client().CreateResource(ctx, coordinationNodePath, resource)
	})
}

func (r *lazyRatelimiter) AlterResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return r.client().AlterResource(ctx, coordinationNodePath, resource)
	})
}

func (r *lazyRatelimiter) DropResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (err error) {
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
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		paths, err = r.client().ListResource(ctx, coordinationNodePath, resourcePath, recursive)
		return xerrors.WithStackTrace(err)
	})
	return paths, xerrors.WithStackTrace(err)
}

func (r *lazyRatelimiter) DescribeResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (resource *ratelimiter.Resource, err error) {
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		resource, err = r.client().DescribeResource(ctx, coordinationNodePath, resourcePath)
		return xerrors.WithStackTrace(err)
	})
	return resource, xerrors.WithStackTrace(err)
}

func (r *lazyRatelimiter) AcquireResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	amount uint64,
	opts ...options.AcquireOption,
) (err error) {
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return r.client().AcquireResource(ctx, coordinationNodePath, resourcePath, amount, opts...)
	})
}

func (r *lazyRatelimiter) client() ratelimiter.Client {
	r.m.Lock()
	defer r.m.Unlock()
	if r.c == nil {
		r.c = builder.New(r.db, r.options)
	}
	return r.c
}
