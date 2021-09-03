package ratelimiter

import (
	"context"
	"google.golang.org/protobuf/proto"

	"github.com/YandexDatabase/ydb-go-genproto/Ydb_RateLimiter_V1"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_RateLimiter"
	"github.com/YandexDatabase/ydb-go-sdk/v2"
)

type HierarchicalDrrSettings struct {
	MaxUnitsPerSecond       float64
	MaxBurstSizeCoefficient float64
	PrefetchCoefficient     float64
	PrefetchWatermark       float64
}

type Resource struct {
	ResourcePath    string
	HierarchicalDrr HierarchicalDrrSettings
}

type Client struct {
	ratelimiterService Ydb_RateLimiter_V1.RateLimiterServiceClient
}

func NewClient(cluster ydb.Cluster) *Client {
	return &Client{
		ratelimiterService: Ydb_RateLimiter_V1.NewRateLimiterServiceClient(cluster),
	}
}

func (c *Client) CreateResource(ctx context.Context, coordinationNodePath string, resource Resource) (err error) {
	_, err = c.ratelimiterService.CreateResource(ctx, &Ydb_RateLimiter.CreateResourceRequest{
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

func (c *Client) AlterResource(ctx context.Context, coordinationNodePath string, resource Resource) (err error) {
	_, err = c.ratelimiterService.AlterResource(ctx, &Ydb_RateLimiter.AlterResourceRequest{
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

func (c *Client) DropResource(ctx context.Context, coordinationNodePath string, resourcePath string) (err error) {
	_, err = c.ratelimiterService.DropResource(ctx, &Ydb_RateLimiter.DropResourceRequest{
		OperationParams:      nil,
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	})
	return
}

func (c *Client) ListResource(ctx context.Context, coordinationNodePath string, resourcePath string, recursive bool) (_ []string, err error) {
	var (
		response *Ydb_RateLimiter.ListResourcesResponse
		result   Ydb_RateLimiter.ListResourcesResult
	)
	response, err = c.ratelimiterService.ListResources(ctx, &Ydb_RateLimiter.ListResourcesRequest{
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

func (c *Client) DescribeResource(ctx context.Context, coordinationNodePath string, resourcePath string) (_ *Resource, err error) {
	var (
		response *Ydb_RateLimiter.DescribeResourceResponse
		result   Ydb_RateLimiter.DescribeResourceResult
	)
	response, err = c.ratelimiterService.DescribeResource(ctx, &Ydb_RateLimiter.DescribeResourceRequest{
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

	resource := &Resource{
		ResourcePath: result.GetResource().GetResourcePath(),
	}

	if result.GetResource().GetHierarchicalDrr() != nil {
		resource.HierarchicalDrr = HierarchicalDrrSettings{
			MaxUnitsPerSecond:       result.GetResource().GetHierarchicalDrr().GetMaxUnitsPerSecond(),
			MaxBurstSizeCoefficient: result.GetResource().GetHierarchicalDrr().GetMaxBurstSizeCoefficient(),
			PrefetchCoefficient:     result.GetResource().GetHierarchicalDrr().GetPrefetchCoefficient(),
			PrefetchWatermark:       result.GetResource().GetHierarchicalDrr().GetPrefetchWatermark(),
		}
	}

	return resource, nil
}

func (c *Client) AcquireResource(ctx context.Context, coordinationNodePath string, resourcePath string, amount uint64, isUsedAmount bool) (err error) {
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
	_, err = c.ratelimiterService.AcquireResource(ctx, &request)
	return
}
