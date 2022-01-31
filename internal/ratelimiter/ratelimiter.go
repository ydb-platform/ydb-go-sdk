package ratelimiter

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_RateLimiter_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_RateLimiter"

	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
)

type Client interface {
	CreateResource(
		ctx context.Context,
		coordinationNodePath string,
		resource ydb_ratelimiter.Resource,
	) (err error)
	AlterResource(
		ctx context.Context,
		coordinationNodePath string,
		resource ydb_ratelimiter.Resource,
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
	) (_ *ydb_ratelimiter.Resource, err error)
	AcquireResource(
		ctx context.Context,
		coordinationNodePath string,
		resourcePath string,
		amount uint64,
		isUsedAmount bool,
	) (err error)
	Close(ctx context.Context) error
}

type client struct {
	service Ydb_RateLimiter_V1.RateLimiterServiceClient
}

func (c *client) Close(ctx context.Context) error {
	return nil
}

func New(cc grpc.ClientConnInterface) Client {
	return &client{
		service: Ydb_RateLimiter_V1.NewRateLimiterServiceClient(cc),
	}
}

func (c *client) CreateResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ydb_ratelimiter.Resource,
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
	})
	return
}

func (c *client) AlterResource(
	ctx context.Context,
	coordinationNodePath string,
	resource ydb_ratelimiter.Resource,
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
	})
	return
}

func (c *client) DropResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (err error) {
	_, err = c.service.DropResource(ctx, &Ydb_RateLimiter.DropResourceRequest{
		OperationParams:      nil,
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	})
	return
}

func (c *client) ListResource(
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
	})
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return nil, err
	}
	return result.GetResourcePaths(), nil
}

func (c *client) DescribeResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
) (_ *ydb_ratelimiter.Resource, err error) {
	var (
		response *Ydb_RateLimiter.DescribeResourceResponse
		result   Ydb_RateLimiter.DescribeResourceResult
	)
	response, err = c.service.DescribeResource(ctx, &Ydb_RateLimiter.DescribeResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	})
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return nil, err
	}

	resource := &ydb_ratelimiter.Resource{
		ResourcePath: result.GetResource().GetResourcePath(),
	}

	if result.GetResource().GetHierarchicalDrr() != nil {
		resource.HierarchicalDrr = ydb_ratelimiter.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       result.GetResource().GetHierarchicalDrr().GetMaxUnitsPerSecond(),
			MaxBurstSizeCoefficient: result.GetResource().GetHierarchicalDrr().GetMaxBurstSizeCoefficient(),
			PrefetchCoefficient:     result.GetResource().GetHierarchicalDrr().GetPrefetchCoefficient(),
			PrefetchWatermark:       result.GetResource().GetHierarchicalDrr().GetPrefetchWatermark(),
		}
	}

	return resource, nil
}

func (c *client) AcquireResource(
	ctx context.Context,
	coordinationNodePath string,
	resourcePath string,
	amount uint64,
	isUsedAmount bool,
) (err error) {
	var request Ydb_RateLimiter.AcquireResourceRequest
	if isUsedAmount {
		request = Ydb_RateLimiter.AcquireResourceRequest{
			CoordinationNodePath: coordinationNodePath,
			ResourcePath:         resourcePath,
			Units:                &Ydb_RateLimiter.AcquireResourceRequest_Used{Used: amount},
		}
	} else {
		request = Ydb_RateLimiter.AcquireResourceRequest{
			CoordinationNodePath: coordinationNodePath,
			ResourcePath:         resourcePath,
			Units:                &Ydb_RateLimiter.AcquireResourceRequest_Required{Required: amount},
		}
	}
	_, err = c.service.AcquireResource(ctx, &request)
	return
}
