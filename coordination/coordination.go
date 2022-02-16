package coordination

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type Client interface {
	closer.Closer

	CreateNode(ctx context.Context, path string, config NodeConfig) (err error)
	AlterNode(ctx context.Context, path string, config NodeConfig) (err error)
	DropNode(ctx context.Context, path string) (err error)
	DescribeNode(ctx context.Context, path string) (_ *scheme.Entry, _ *NodeConfig, err error)
}
