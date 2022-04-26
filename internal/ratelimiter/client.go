package ratelimiter

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_RateLimiter_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_RateLimiter"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/config"
	ratelimiterErrors "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

var (
	errUnknownAcquireType = xerrors.Wrap(errors.New("unknown acquire type"))
	errNilClient          = xerrors.Wrap(errors.New("ratelimiter client is not initialized"))
)

type Client struct {
	config  config.Config
	service Ydb_RateLimiter_V1.RateLimiterServiceClient
}

func (c *Client) Close(ctx context.Context) error {
	return nil
}

func New(cc grpc.ClientConnInterface, config config.Config) *Client {
	return &Client{
		config:  config,
		service: Ydb_RateLimiter_V1.NewRateLimiterServiceClient(cc),
	}
}

func (c *Client) CreateResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		return xerrors.WithStackTrace(c.createResource(ctx, coordinationNodePath, resource))
	}
	return xerrors.WithStackTrace(retry.Retry(ctx, func(ctx context.Context) (err error) {
		return c.createResource(ctx, coordinationNodePath, resource)
	}))
}

func (c *Client) createResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	_, err = c.service.CreateResource(ctx, &Ydb_RateLimiter.CreateResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		Resource: &Ydb_RateLimiter.Resource{
			ResourcePath: resource.ResourcePath,
			Type: &Ydb_RateLimiter.Resource_HierarchicalDrr{HierarchicalDrr: &Ydb_RateLimiter.HierarchicalDrrSettings{
				MaxUnitsPerSecond:       resource.HierarchicalDrr.MaxUnitsPerSecond,
				MaxBurstSizeCoefficient: resource.HierarchicalDrr.MaxBurstSizeCoefficient,
				PrefetchCoefficient:     resource.HierarchicalDrr.PrefetchCoefficient,
				PrefetchWatermark:       resource.HierarchicalDrr.PrefetchWatermark,
			}},
		},
		OperationParams: operation.Params(
			ctx,
			c.config.OperationTimeout(),
			c.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	})
	return
}

func (c *Client) AlterResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		return xerrors.WithStackTrace(c.alterResource(ctx, coordinationNodePath, resource))
	}
	return xerrors.WithStackTrace(retry.Retry(ctx, func(ctx context.Context) (err error) {
		return xerrors.WithStackTrace(c.alterResource(ctx, coordinationNodePath, resource))
	}))
}

func (c *Client) alterResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ratelimiter.Resource,
) (err error) {
	_, err = c.service.AlterResource(ctx, &Ydb_RateLimiter.AlterResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		Resource: &Ydb_RateLimiter.Resource{
			ResourcePath: resource.ResourcePath,
			Type: &Ydb_RateLimiter.Resource_HierarchicalDrr{HierarchicalDrr: &Ydb_RateLimiter.HierarchicalDrrSettings{
				MaxUnitsPerSecond:       resource.HierarchicalDrr.MaxUnitsPerSecond,
				MaxBurstSizeCoefficient: resource.HierarchicalDrr.MaxBurstSizeCoefficient,
				PrefetchCoefficient:     resource.HierarchicalDrr.PrefetchCoefficient,
				PrefetchWatermark:       resource.HierarchicalDrr.PrefetchWatermark,
			}},
		},
		OperationParams: operation.Params(
			ctx,
			c.config.OperationTimeout(),
			c.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	})
	return
}

func (c *Client) DropResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		return xerrors.WithStackTrace(c.dropResource(ctx, coordinationNodePath, resourcePath))
	}
	return xerrors.WithStackTrace(retry.Retry(ctx, func(ctx context.Context) (err error) {
		return xerrors.WithStackTrace(c.dropResource(ctx, coordinationNodePath, resourcePath))
	}))
}

func (c *Client) dropResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (err error) {
	_, err = c.service.DropResource(ctx, &Ydb_RateLimiter.DropResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
		OperationParams: operation.Params(
			ctx,
			c.config.OperationTimeout(),
			c.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	})
	return
}

func (c *Client) ListResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	recursive bool,
) (list []string, err error) {
	if c == nil {
		return list, xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		list, err = c.listResource(ctx, coordinationNodePath, resourcePath, recursive)
		return list, xerrors.WithStackTrace(err)
	}
	err = xerrors.WithStackTrace(retry.Retry(ctx, func(ctx context.Context) (err error) {
		list, err = c.listResource(ctx, coordinationNodePath, resourcePath, recursive)
		return xerrors.WithStackTrace(err)
	}, retry.WithIdempotent(true)))
	return
}

func (c *Client) listResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	recursive bool,
) (_ []string, err error) {
	var (
		response *Ydb_RateLimiter.ListResourcesResponse
		result   Ydb_RateLimiter.ListResourcesResult
	)
	response, err = c.service.ListResources(ctx, &Ydb_RateLimiter.ListResourcesRequest{
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
		Recursive:            recursive,
		OperationParams: operation.Params(
			ctx,
			c.config.OperationTimeout(),
			c.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return result.GetResourcePaths(), nil
}

func (c *Client) DescribeResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (resource *ratelimiter.Resource, err error) {
	if c == nil {
		return resource, xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		resource, err = c.describeResource(ctx, coordinationNodePath, resourcePath)
		return resource, xerrors.WithStackTrace(err)
	}
	err = xerrors.WithStackTrace(retry.Retry(ctx, func(ctx context.Context) (err error) {
		resource, err = c.describeResource(ctx, coordinationNodePath, resourcePath)
		return xerrors.WithStackTrace(err)
	}, retry.WithIdempotent(true)))
	return
}

func (c *Client) describeResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (_ *ratelimiter.Resource, err error) {
	var (
		response *Ydb_RateLimiter.DescribeResourceResponse
		result   Ydb_RateLimiter.DescribeResourceResult
	)
	response, err = c.service.DescribeResource(ctx, &Ydb_RateLimiter.DescribeResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
		OperationParams: operation.Params(
			ctx,
			c.config.OperationTimeout(),
			c.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	resource := &ratelimiter.Resource{
		ResourcePath: result.GetResource().GetResourcePath(),
	}

	if result.GetResource().GetHierarchicalDrr() != nil {
		resource.HierarchicalDrr = ratelimiter.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       result.GetResource().GetHierarchicalDrr().GetMaxUnitsPerSecond(),
			MaxBurstSizeCoefficient: result.GetResource().GetHierarchicalDrr().GetMaxBurstSizeCoefficient(),
			PrefetchCoefficient:     result.GetResource().GetHierarchicalDrr().GetPrefetchCoefficient(),
			PrefetchWatermark:       result.GetResource().GetHierarchicalDrr().GetPrefetchWatermark(),
		}
	}

	return resource, nil
}

func (c *Client) AcquireResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	amount uint64,
	opts ...options.AcquireOption,
) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	if !c.config.AutoRetry() {
		return xerrors.WithStackTrace(c.acquireResource(ctx, coordinationNodePath, resourcePath, amount, opts...))
	}
	return xerrors.WithStackTrace(retry.Retry(ctx, func(ctx context.Context) (err error) {
		return xerrors.WithStackTrace(c.acquireResource(ctx, coordinationNodePath, resourcePath, amount, opts...))
	}))
}

func (c *Client) acquireResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	amount uint64,
	opts ...options.AcquireOption,
) (err error) {
	acquireOptions := options.NewAcquire(
		append(
			[]options.AcquireOption{
				options.WithOperationTimeout(c.config.OperationTimeout()),
				options.WithOperationCancelAfter(c.config.OperationCancelAfter()),
			},
			opts...,
		)...,
	)

	switch acquireOptions.Type() {
	case options.AcquireTypeAcquire:
		_, err = c.service.AcquireResource(
			ctx,
			&Ydb_RateLimiter.AcquireResourceRequest{
				CoordinationNodePath: coordinationNodePath,
				ResourcePath:         resourcePath,
				Units: &Ydb_RateLimiter.AcquireResourceRequest_Required{
					Required: amount,
				},
				OperationParams: operation.Params(
					ctx,
					acquireOptions.OperationTimeout(),
					acquireOptions.OperationCancelAfter(),
					operation.ModeSync,
				),
			},
		)
	case options.AcquireTypeReport:
		_, err = c.service.AcquireResource(
			ctx,
			&Ydb_RateLimiter.AcquireResourceRequest{
				CoordinationNodePath: coordinationNodePath,
				ResourcePath:         resourcePath,
				Units: &Ydb_RateLimiter.AcquireResourceRequest_Used{
					Used: amount,
				},
				OperationParams: operation.Params(
					ctx,
					acquireOptions.OperationTimeout(),
					acquireOptions.OperationCancelAfter(),
					operation.ModeSync,
				),
			},
		)
	default:
		return xerrors.WithStackTrace(fmt.Errorf("%w: %d", errUnknownAcquireType, acquireOptions.Type()))
	}

	if xerrors.IsOperationError(err, Ydb.StatusIds_TIMEOUT, Ydb.StatusIds_CANCELLED) {
		return xerrors.WithStackTrace(ratelimiterErrors.NewAcquire(amount, err))
	}

	return xerrors.WithStackTrace(err)
}
