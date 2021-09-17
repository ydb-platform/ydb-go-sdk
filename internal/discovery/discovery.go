package discovery

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"google.golang.org/protobuf/proto"
)

type Client interface {
	Discover(ctx context.Context) ([]cluster.Endpoint, error)
}

func New(conn grpc.ClientConnInterface, database string, tls bool) Client {
	return &client{
		discoveryService: Ydb_Discovery_V1.NewDiscoveryServiceClient(conn),
		database:         database,
		ssl:              tls,
	}
}

type client struct {
	discoveryService Ydb_Discovery_V1.DiscoveryServiceClient
	database         string
	ssl              bool
}

func (d *client) Discover(ctx context.Context) ([]cluster.Endpoint, error) {
	request := Ydb_Discovery.ListEndpointsRequest{
		Database: d.database,
	}
	response, err := d.discoveryService.ListEndpoints(ctx, &request)
	if err != nil {
		return nil, err
	}
	listEndpointsResult := Ydb_Discovery.ListEndpointsResult{}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &listEndpointsResult)
	if err != nil {
		return nil, err
	}
	endpoints := make([]cluster.Endpoint, 0, len(listEndpointsResult.Endpoints))
	for _, e := range listEndpointsResult.Endpoints {
		if e.Ssl == d.ssl {
			endpoints = append(endpoints, cluster.Endpoint{
				Addr:  e.Address,
				Port:  int(e.Port),
				Local: e.Location == listEndpointsResult.SelfLocation,
			})
		}
	}
	return endpoints, nil
}
