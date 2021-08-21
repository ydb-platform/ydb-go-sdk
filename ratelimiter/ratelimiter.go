package ratelimiter

import (
	"context"
	"google.golang.org/protobuf/proto"

	"github.com/YandexDatabase/ydb-go-genproto/Ydb_RateLimiter_V1"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_RateLimiter"
	ydb "github.com/YandexDatabase/ydb-go-sdk/v2"
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

type client struct {
	cluster ydb.Cluster
}

func NewClient(cluster ydb.Cluster) *client {
	return &client{cluster: cluster}
}

func (c *client) CreateResource(ctx context.Context, coordinationNodePath string, resource Resource) (err error) {
	request := Ydb_RateLimiter.CreateResourceRequest{
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
	}
	conn, err := c.cluster.Get(ctx)
	if err != nil {
		return err
	}
	_, err = Ydb_RateLimiter_V1.NewRateLimiterServiceClient(conn).CreateResource(ctx, &request)
	return err
}

func (c *client) AlterResource(ctx context.Context, coordinationNodePath string, resource Resource) (err error) {
	request := Ydb_RateLimiter.AlterResourceRequest{
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
	}
	conn, err := c.cluster.Get(ctx)
	if err != nil {
		return err
	}
	_, err = Ydb_RateLimiter_V1.NewRateLimiterServiceClient(conn).AlterResource(ctx, &request)
	return err
}

func (c *client) DropResource(ctx context.Context, coordinationNodePath string, resourcePath string) (err error) {
	request := Ydb_RateLimiter.DropResourceRequest{
		OperationParams:      nil,
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	}
	conn, err := c.cluster.Get(ctx)
	if err != nil {
		return err
	}
	_, err = Ydb_RateLimiter_V1.NewRateLimiterServiceClient(conn).DropResource(ctx, &request)
	return err
}

func (c *client) ListResource(ctx context.Context, coordinationNodePath string, resourcePath string, recursive bool) ([]string, error) {
	var resourceList Ydb_RateLimiter.ListResourcesResult
	request := Ydb_RateLimiter.ListResourcesRequest{
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	}
	conn, err := c.cluster.Get(ctx)
	if err != nil {
		return nil, err
	}
	response, err := Ydb_RateLimiter_V1.NewRateLimiterServiceClient(conn).ListResources(ctx, &request)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &resourceList)
	if err != nil {
		return nil, err
	}
	return resourceList.GetResourcePaths(), nil
}

func (c *client) DescribeResource(ctx context.Context, coordinationNodePath string, resourcePath string) (*Resource, error) {
	var resourceResult Ydb_RateLimiter.DescribeResourceResult
	request := Ydb_RateLimiter.DescribeResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	}
	conn, err := c.cluster.Get(ctx)
	if err != nil {
		return nil, err
	}
	response, err := Ydb_RateLimiter_V1.NewRateLimiterServiceClient(conn).DescribeResource(ctx, &request)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &resourceResult)
	if err != nil {
		return nil, err
	}
	result := &Resource{
		ResourcePath: resourceResult.Resource.ResourcePath,
	}
	if resourceResult.GetResource().GetHierarchicalDrr() != nil {
		result.HierarchicalDrr = HierarchicalDrrSettings{
			MaxUnitsPerSecond:       resourceResult.GetResource().GetHierarchicalDrr().MaxUnitsPerSecond,
			MaxBurstSizeCoefficient: resourceResult.GetResource().GetHierarchicalDrr().MaxBurstSizeCoefficient,
			PrefetchCoefficient:     resourceResult.GetResource().GetHierarchicalDrr().PrefetchCoefficient,
			PrefetchWatermark:       resourceResult.GetResource().GetHierarchicalDrr().PrefetchWatermark,
		}
	}
	return result, nil
}

func (c *client) AcquireResource(ctx context.Context, coordinationNodePath string, resourcePath string, amount uint64, isUsedAmount bool) (err error) {
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
	conn, err := c.cluster.Get(ctx)
	if err != nil {
		return err
	}
	_, err = Ydb_RateLimiter_V1.NewRateLimiterServiceClient(conn).AcquireResource(ctx, &request)
	return err
}
