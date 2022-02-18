package ratelimiter

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/options"
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
		opts ...options.AcquireOption,
	) (err error)
}

func WithTimeout(timeout time.Duration) options.AcquireOption {
	return options.WithTimeout(timeout)
}

func WithAcquire() options.AcquireOption {
	return options.WithAcquire()
}

func WithReportAsync() options.AcquireOption {
	return options.WithReportAsync()
}

func WithReportSync() options.AcquireOption {
	return options.WithReportAsync()
}
