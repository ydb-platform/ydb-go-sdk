package discovery

import (
	"context"
	"net"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"

	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func New(
	conn grpc.ClientConnInterface,
	endpoint, database string,
	ssl bool,
	trace ydb_trace.Driver,
) ydb_discovery.Client {
	return &client{
		trace:    trace,
		endpoint: endpoint,
		database: database,
		ssl:      ssl,
		service:  Ydb_Discovery_V1.NewDiscoveryServiceClient(conn),
	}
}

type client struct {
	trace    ydb_trace.Driver
	endpoint string
	database string
	ssl      bool
	service  Ydb_Discovery_V1.DiscoveryServiceClient
}

func (d *client) Discover(ctx context.Context) (endpoints []endpoint.Endpoint, err error) {
	onDone := ydb_trace.DriverOnDiscovery(d.trace, &ctx, d.endpoint)
	var location string
	defer func() {
		nodes := make([]string, 0)
		for _, e := range endpoints {
			nodes = append(nodes, e.Address())
		}
		onDone(location, nodes, err)
	}()
	request := Ydb_Discovery.ListEndpointsRequest{
		Database: d.database,
	}
	response, err := d.service.ListEndpoints(ctx, &request)
	if err != nil {
		return nil, err
	}
	result := Ydb_Discovery.ListEndpointsResult{}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return nil, err
	}
	location = result.GetSelfLocation()
	endpoints = make([]endpoint.Endpoint, 0, len(result.Endpoints))
	for _, e := range result.Endpoints {
		if e.Ssl == d.ssl {
			endpoints = append(endpoints, endpoint.New(
				net.JoinHostPort(e.GetAddress(), strconv.Itoa(int(e.GetPort()))),
				endpoint.WithLocation(e.GetLocation()),
				endpoint.WithID(e.GetNodeId()),
				endpoint.WithLoadFactor(e.GetLoadFactor()),
				endpoint.WithLocalDC(e.GetLocation() == location),
			))
		}
	}
	return endpoints, nil
}

func (d *client) WhoAmI(ctx context.Context) (*ydb_discovery.WhoAmI, error) {
	request := Ydb_Discovery.WhoAmIRequest{}
	response, err := d.service.WhoAmI(ctx, &request)
	if err != nil {
		return nil, err
	}
	whoAmIResultResult := Ydb_Discovery.WhoAmIResult{}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &whoAmIResultResult)
	if err != nil {
		return nil, err
	}
	return &ydb_discovery.WhoAmI{
		User:   whoAmIResultResult.GetUser(),
		Groups: whoAmIResultResult.GetGroups(),
	}, nil
}

func (d *client) Close(context.Context) error {
	return nil
}
