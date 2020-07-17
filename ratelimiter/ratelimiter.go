package ratelimiter

import (
	"context"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/api"
	"github.com/yandex-cloud/ydb-go-sdk/api/grpc/Ydb_RateLimiter_V1"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_RateLimiter"
)

type HierarchicalDrrSettings struct {
	MaxUnitsPerSecond       float64
	MaxBurstSizeCoefficient float64
}

type Resource struct {
	ResourcePath    string
	HierarchicalDrr HierarchicalDrrSettings
}

type Client struct {
	Driver ydb.Driver
}

func (c *Client) CreateResource(ctx context.Context, coordinationNodePath string, resource Resource) error {
	req := Ydb_RateLimiter.CreateResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		Resource: &Ydb_RateLimiter.Resource{
			ResourcePath: resource.ResourcePath,
			Type: &Ydb_RateLimiter.Resource_HierarchicalDrr{HierarchicalDrr: &Ydb_RateLimiter.HierarchicalDrrSettings{
				MaxUnitsPerSecond:       resource.HierarchicalDrr.MaxUnitsPerSecond,
				MaxBurstSizeCoefficient: resource.HierarchicalDrr.MaxBurstSizeCoefficient,
			}},
		},
	}
	return c.Driver.Call(ctx, api.Wrap(
		Ydb_RateLimiter_V1.CreateResource, &req, nil,
	))
}

func (c *Client) AlterResource(ctx context.Context, coordinationNodePath string, resource Resource) error {
	req := Ydb_RateLimiter.AlterResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		Resource: &Ydb_RateLimiter.Resource{
			ResourcePath: resource.ResourcePath,
			Type: &Ydb_RateLimiter.Resource_HierarchicalDrr{HierarchicalDrr: &Ydb_RateLimiter.HierarchicalDrrSettings{
				MaxUnitsPerSecond:       resource.HierarchicalDrr.MaxUnitsPerSecond,
				MaxBurstSizeCoefficient: resource.HierarchicalDrr.MaxBurstSizeCoefficient,
			}},
		},
	}
	return c.Driver.Call(ctx, api.Wrap(
		Ydb_RateLimiter_V1.AlterResource, &req, nil,
	))
}

func (c *Client) DropResource(ctx context.Context, coordinationNodePath string, resourcePath string) error {
	req := Ydb_RateLimiter.DropResourceRequest{
		OperationParams:      nil,
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	}
	return c.Driver.Call(ctx, api.Wrap(
		Ydb_RateLimiter_V1.DropResource, &req, nil,
	))
}

func (c *Client) ListResource(ctx context.Context, coordinationNodePath string, resourcePath string, recursive bool) ([]string, error) {
	var res Ydb_RateLimiter.ListResourcesResult
	req := Ydb_RateLimiter.ListResourcesRequest{
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	}
	err := c.Driver.Call(ctx, api.Wrap(
		Ydb_RateLimiter_V1.ListResources, &req, &res,
	))
	if err != nil {
		return nil, err
	}
	return res.ResourcePaths, nil
}

func (c *Client) DescribeResource(ctx context.Context, coordinationNodePath string, resourcePath string) (*Resource, error) {
	var res Ydb_RateLimiter.DescribeResourceResult
	req := Ydb_RateLimiter.DescribeResourceRequest{
		CoordinationNodePath: coordinationNodePath,
		ResourcePath:         resourcePath,
	}
	err := c.Driver.Call(ctx, api.Wrap(
		Ydb_RateLimiter_V1.DescribeResource, &req, &res,
	))
	if err != nil {
		return nil, err
	}

	result := &Resource{
		ResourcePath: res.Resource.ResourcePath,
	}

	if res.Resource.GetHierarchicalDrr() != nil {
		result.HierarchicalDrr = HierarchicalDrrSettings{
			MaxUnitsPerSecond:       res.Resource.GetHierarchicalDrr().MaxUnitsPerSecond,
			MaxBurstSizeCoefficient: res.Resource.GetHierarchicalDrr().MaxBurstSizeCoefficient,
		}
	}

	return result, nil
}

func (c *Client) AcquireResource(ctx context.Context, coordinationNodePath string, resourcePath string, amount uint64, isUsedAmount bool) error {
	var req Ydb_RateLimiter.AcquireResourceRequest
	if isUsedAmount {
		req = Ydb_RateLimiter.AcquireResourceRequest{
			CoordinationNodePath: coordinationNodePath,
			ResourcePath:         resourcePath,
			Units:                &Ydb_RateLimiter.AcquireResourceRequest_Used{Used: amount},
		}
	} else {
		req = Ydb_RateLimiter.AcquireResourceRequest{
			CoordinationNodePath: coordinationNodePath,
			ResourcePath:         resourcePath,
			Units:                &Ydb_RateLimiter.AcquireResourceRequest_Required{Required: amount},
		}
	}
	return c.Driver.Call(ctx, api.Wrap(
		Ydb_RateLimiter_V1.AcquireResource, &req, nil,
	))
}
