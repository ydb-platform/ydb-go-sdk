package discovery

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type WhoAmI struct {
	User   string
	Groups []string
}

func (w WhoAmI) String() string {
	return fmt.Sprintf("{User: %s, Groups: [%s]}", w.User, strings.Join(w.Groups, ","))
}

type Client interface {
	Discover(ctx context.Context) ([]endpoint.Endpoint, error)
	WhoAmI(ctx context.Context) (*WhoAmI, error)
	Close(ctx context.Context) error
}

func New(conn grpc.ClientConnInterface, endpoint, database string, ssl bool, trace trace.Driver) Client {
	return &client{
		trace:    trace,
		endpoint: endpoint,
		database: database,
		ssl:      ssl,
		service:  Ydb_Discovery_V1.NewDiscoveryServiceClient(conn),
	}
}

type client struct {
	trace    trace.Driver
	endpoint string
	database string
	ssl      bool
	service  Ydb_Discovery_V1.DiscoveryServiceClient
}

func (d *client) Discover(ctx context.Context) (endpoints []endpoint.Endpoint, err error) {
	onDone := trace.DriverOnDiscovery(d.trace, &ctx, d.endpoint)
	defer func() {
		nodes := make([]string, 0)
		for _, e := range endpoints {
			nodes = append(nodes, e.Address())
		}
		onDone(nodes, err)
	}()
	request := Ydb_Discovery.ListEndpointsRequest{
		Database: d.database,
	}
	response, err := d.service.ListEndpoints(ctx, &request)
	if err != nil {
		return nil, err
	}
	listEndpointsResult := Ydb_Discovery.ListEndpointsResult{}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &listEndpointsResult)
	if err != nil {
		return nil, err
	}
	endpoints = make([]endpoint.Endpoint, 0, len(listEndpointsResult.Endpoints))
	for _, e := range listEndpointsResult.Endpoints {
		if e.Ssl == d.ssl {
			endpoints = append(endpoints, endpoint.New(
				net.JoinHostPort(e.GetAddress(), strconv.Itoa(int(e.GetPort()))),
				endpoint.WithID(e.GetNodeId()),
				endpoint.WithLoadFactor(e.GetLoadFactor()),
				endpoint.WithLocalDC(e.GetLocation() == listEndpointsResult.GetSelfLocation()),
			))
		}
	}
	return endpoints, nil
}

func (d *client) WhoAmI(ctx context.Context) (*WhoAmI, error) {
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
	return &WhoAmI{
		User:   whoAmIResultResult.GetUser(),
		Groups: whoAmIResultResult.GetGroups(),
	}, nil
}

func (d *client) Close(context.Context) error {
	return nil
}
