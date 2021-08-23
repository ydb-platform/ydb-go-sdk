package ydb

import (
	"context"
	"github.com/YandexDatabase/ydb-go-genproto/Ydb_Discovery_V1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Discovery"
)

type Endpoint struct {
	Addr       string
	Port       int
	LoadFactor float32
	Local      bool
}

type discoveryClient struct {
	cluster  Cluster
	database string
	ssl      bool
}

func discover(ctx context.Context, database string, ssl bool, conn grpc.ClientConnInterface) ([]Endpoint, error) {
	request := Ydb_Discovery.ListEndpointsRequest{
		Database: database,
	}
	discoveryServiceClient := Ydb_Discovery_V1.NewDiscoveryServiceClient(conn)
	response, err := discoveryServiceClient.ListEndpoints(ctx, &request)
	if err != nil {
		return nil, err
	}
	listEndpointsResult := Ydb_Discovery.ListEndpointsResult{}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &listEndpointsResult)
	if err != nil {
		return nil, err
	}
	endpoints := make([]Endpoint, 0, len(listEndpointsResult.Endpoints))
	for _, e := range listEndpointsResult.Endpoints {
		if e.Ssl == ssl {
			endpoints = append(endpoints, Endpoint{
				Addr:  e.Address,
				Port:  int(e.Port),
				Local: e.Location == listEndpointsResult.SelfLocation,
			})
		}
	}
	return endpoints, nil
}

func (d *discoveryClient) Discover(ctx context.Context) ([]Endpoint, error) {
	conn, err := d.cluster.Get(ctx)
	if err != nil {
		return nil, err
	}
	return discover(ctx, d.database, d.ssl, conn)
}
