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

// WithDecrease specifies decrease timeout from context
// Default value defined in options.DecreaseTimeout
// Example 1:
//   ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//   defer cancel()
//   err := db.Ratelimiter().AcquireResource(
//     ctx,
//     "/path",
//     "resource",
//     1000,
//     ratelimiter.WithAcquire(),
//     ratelimiter.WithDecrease(100 * time.Millisecond),
//   )
// Request to ratelimiter service will have:
//   - context with timeout 1000ms
//   - cancelAfter timeout (1000-100)ms
//   - operation timeout - from main config
//
// Example 2:
//   ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//   defer cancel()
//   err := db.Ratelimiter().AcquireResource(
//     ctx,
//     "/path",
//     "resource",
//     1000,
//     ratelimiter.WithReportSync(),
//     ratelimiter.WithDecrease(100 * time.Millisecond),
//   )
// Request to ratelimiter service will have:
//   - context with timeout 1000ms
//   - operation timeout (1000-100)ms
//   - cancelAfter timeout - from main config
//
// Example 3:
//   ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//   defer cancel()
//   err := db.Ratelimiter().AcquireResource(
//     ctx,
//     "/path",
//     "resource",
//     1000,
//     ratelimiter.WithReportAsync(),
//     ratelimiter.WithDecrease(100 * time.Millisecond), // nop
//   )
// Request to ratelimiter service will have:
//   - context with timeout 1000ms
//   - operation timeout and cancelAfter timeout - from main config
func WithDecrease(decreaseTimeout time.Duration) options.AcquireOption {
	return options.WithDecrease(decreaseTimeout)
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
