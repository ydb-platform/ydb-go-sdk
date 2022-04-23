package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/database"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type lazyCoordination struct {
	db     database.Connection
	config config.Config
	c      coordination.Client
	m      sync.Mutex
}

func Coordination(db database.Connection, options []config.Option) coordination.Client {
	return &lazyCoordination{
		db:     db,
		config: config.New(options...),
	}
}

func (c *lazyCoordination) CreateNode(ctx context.Context, path string, config coordination.NodeConfig) (err error) {
	if !c.config.AutoRetry() {
		return c.client().CreateNode(ctx, path, config)
	}
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return c.client().CreateNode(ctx, path, config)
	})
}

func (c *lazyCoordination) AlterNode(ctx context.Context, path string, config coordination.NodeConfig) (err error) {
	if !c.config.AutoRetry() {
		return c.client().AlterNode(ctx, path, config)
	}
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return c.client().AlterNode(ctx, path, config)
	})
}

func (c *lazyCoordination) DropNode(ctx context.Context, path string) (err error) {
	if !c.config.AutoRetry() {
		return c.client().DropNode(ctx, path)
	}
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return c.client().DropNode(ctx, path)
	})
}

func (c *lazyCoordination) DescribeNode(
	ctx context.Context,
	path string,
) (
	entry *scheme.Entry,
	config *coordination.NodeConfig,
	err error,
) {
	if !c.config.AutoRetry() {
		return c.client().DescribeNode(ctx, path)
	}
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		entry, config, err = c.client().DescribeNode(ctx, path)
		return err
	})
	return entry, config, err
}

func (c *lazyCoordination) Close(ctx context.Context) (err error) {
	c.m.Lock()
	defer c.m.Unlock()
	if c.c == nil {
		return nil
	}
	return c.c.Close(ctx)
}

func (c *lazyCoordination) client() coordination.Client {
	c.m.Lock()
	defer c.m.Unlock()
	if c.c == nil {
		c.c = builder.New(c.db, c.config)
	}
	return c.c
}
