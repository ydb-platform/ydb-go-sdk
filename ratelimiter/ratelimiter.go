// nolint:revive
package ydb_ratelimiter

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
)

type Client interface {
	closer.Closer

	CreateResource(
		ctx context.Context,
		coordinationNodePath string,
		resource Resource,
	) (err error)
	AlterResource(
		ctx context.Context,
		coordinationNodePath string,
		resource Resource,
	) (err error)
	DropResource(
		ctx context.Context,
		coordinationNodePath string,
		resourcePath string,
	) (err error)
	ListResource(
		ctx context.Context,
		coordinationNodePath string,
		resourcePath string,
		recursive bool,
	) (_ []string, err error)
	DescribeResource(
		ctx context.Context,
		coordinationNodePath string,
		resourcePath string,
	) (_ *Resource, err error)
	AcquireResource(
		ctx context.Context,
		coordinationNodePath string,
		resourcePath string,
		amount uint64,
		isUsedAmount bool,
	) (err error)
}
