package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type lazyCoordination struct {
	db     db.Connection
	client ydb_coordination.Client
	m      sync.Mutex
}

func Coordination(db db.Connection) ydb_coordination.Client {
	return &lazyCoordination{
		db: db,
	}
}

func (c *lazyCoordination) CreateNode(ctx context.Context, path string, config ydb_coordination.Config) (err error) {
	c.init()
	return c.client.CreateNode(ctx, path, config)
}

func (c *lazyCoordination) AlterNode(ctx context.Context, path string, config ydb_coordination.Config) (err error) {
	c.init()
	return c.client.AlterNode(ctx, path, config)
}

func (c *lazyCoordination) DropNode(ctx context.Context, path string) (err error) {
	c.init()
	return c.client.DropNode(ctx, path)
}

func (c *lazyCoordination) DescribeNode(
	ctx context.Context,
	path string,
) (
	_ *ydb_scheme.Entry,
	_ *ydb_coordination.Config,
	err error,
) {
	c.init()
	return c.client.DescribeNode(ctx, path)
}

func (c *lazyCoordination) Close(ctx context.Context) error {
	c.m.Lock()
	defer c.m.Unlock()
	if c.client == nil {
		return nil
	}
	defer func() {
		c.client = nil
	}()
	return c.client.Close(ctx)
}

func (c *lazyCoordination) init() {
	c.m.Lock()
	if c.client == nil {
		c.client = coordination.New(c.db)
	}
	c.m.Unlock()
}
