package proxy

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type proxyCoordination struct {
	client ydb_coordination.Client
	meta   meta.Meta
}

func Coordination(client ydb_coordination.Client, meta meta.Meta) ydb_coordination.Client {
	return &proxyCoordination{
		client: client,
		meta:   meta,
	}
}

func (c *proxyCoordination) CreateNode(ctx context.Context, path string, config ydb_coordination.Config) (err error) {
	ctx, err = c.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return c.client.CreateNode(ctx, path, config)
}

func (c *proxyCoordination) AlterNode(ctx context.Context, path string, config ydb_coordination.Config) (err error) {
	ctx, err = c.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return c.client.AlterNode(ctx, path, config)
}

func (c *proxyCoordination) DropNode(ctx context.Context, path string) (err error) {
	ctx, err = c.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return c.client.DropNode(ctx, path)
}

func (c *proxyCoordination) DescribeNode(
	ctx context.Context,
	path string,
) (
	_ *ydb_scheme.Entry,
	_ *ydb_coordination.Config,
	err error,
) {
	ctx, err = c.meta.Meta(ctx)
	if err != nil {
		return nil, nil, err
	}
	return c.client.DescribeNode(ctx, path)
}

func (c *proxyCoordination) Close(ctx context.Context) (err error) {
	ctx, err = c.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return c.client.Close(ctx)
}
