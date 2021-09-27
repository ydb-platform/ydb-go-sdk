package discovery

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/endpoint"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"strings"
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

func New(conn grpc.ClientConnInterface, database string, ssl bool) Client {
	return &client{
		service:  Ydb_Discovery_V1.NewDiscoveryServiceClient(conn),
		database: database,
		ssl:      ssl,
	}
}

type client struct {
	service  Ydb_Discovery_V1.DiscoveryServiceClient
	database string
	ssl      bool
}

func (d *client) Discover(ctx context.Context) ([]endpoint.Endpoint, error) {
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
	endpoints := make([]endpoint.Endpoint, 0, len(listEndpointsResult.Endpoints))
	for _, e := range listEndpointsResult.Endpoints {
		if e.Ssl == d.ssl {
			endpoints = append(endpoints, endpoint.Endpoint{
				Addr: endpoint.Addr{
					Host: e.Address,
					Port: int(e.Port),
				},
				Local: e.Location == listEndpointsResult.SelfLocation,
			})
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
