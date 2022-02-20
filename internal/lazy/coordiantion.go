package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination/config"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type lazyCoordination struct {
	db      db.Connection
	options []config.Option
	client  coordination.Client
	m       sync.Mutex
}

func Coordination(db db.Connection, options []config.Option) coordination.Client {
	return &lazyCoordination{
		db:      db,
		options: options,
	}
}

func (c *lazyCoordination) CreateNode(ctx context.Context, path string, config coordination.NodeConfig) (err error) {
	c.init()
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return c.client.CreateNode(ctx, path, config)
	})
}

func (c *lazyCoordination) AlterNode(ctx context.Context, path string, config coordination.NodeConfig) (err error) {
	c.init()
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return c.client.AlterNode(ctx, path, config)
	})
}

func (c *lazyCoordination) DropNode(ctx context.Context, path string) (err error) {
	c.init()
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return c.client.DropNode(ctx, path)
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
	c.init()
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		entry, config, err = c.client.DescribeNode(ctx, path)
		return err
	})
	return entry, config, err
}

func (c *lazyCoordination) Close(ctx context.Context) (err error) {
	c.m.Lock()
	defer c.m.Unlock()
	if c.client == nil {
		return nil
	}
	defer func() {
		c.client = nil
	}()
	err = c.client.Close(ctx)
	if err != nil {
		return errors.Errorf(0, "close failed: %w", err)
	}
	return nil
}

func (c *lazyCoordination) init() {
	c.m.Lock()
	if c.client == nil {
		c.client = builder.New(c.db, c.options)
	}
	c.m.Unlock()
}
