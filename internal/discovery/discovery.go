package discovery

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var DefaultDiscoveryInterval = time.Minute

func New(
	config config.Config,
	grpcOptions ...grpc.DialOption,
) *Client {
	return &Client{
		config:      config,
		grpcOptions: grpcOptions,
	}
}

var _ discovery.Client = &Client{}

type Client struct {
	config      config.Config
	grpcOptions []grpc.DialOption
}

func (c *Client) client(ctx context.Context) (Ydb_Discovery_V1.DiscoveryServiceClient, *grpc.ClientConn, error) {
	cc, err := grpc.DialContext(ctx,
		"dns:///"+c.config.Endpoint(),
		c.grpcOptions...,
	)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}
	return Ydb_Discovery_V1.NewDiscoveryServiceClient(cc), cc, nil
}

// Discover cluster endpoints
func (c *Client) Discover(ctx context.Context) (endpoints []endpoint.Endpoint, err error) {
	var (
		onDone  = trace.DiscoveryOnDiscover(c.config.Trace(), &ctx, c.config.Endpoint(), c.config.Database())
		request = Ydb_Discovery.ListEndpointsRequest{
			Database: c.config.Database(),
		}
		response *Ydb_Discovery.ListEndpointsResponse
		result   Ydb_Discovery.ListEndpointsResult
	)

	var location string
	defer func() {
		nodes := make([]trace.EndpointInfo, 0, len(endpoints))
		for _, e := range endpoints {
			nodes = append(nodes, e.Copy())
		}
		onDone(location, nodes, err)
	}()

	ctx, err = c.config.Meta().Context(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	client, cc, err := c.client(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	defer func() {
		_ = cc.Close()
	}()

	response, err = client.ListEndpoints(ctx, &request)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	err = response.GetOperation().GetResult().UnmarshalTo(&result)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	location = result.GetSelfLocation()
	endpoints = make([]endpoint.Endpoint, 0, len(result.Endpoints))
	for _, e := range result.Endpoints {
		if e.Ssl == c.config.Secure() {
			endpoints = append(endpoints, endpoint.New(
				net.JoinHostPort(e.GetAddress(), strconv.Itoa(int(e.GetPort()))),
				endpoint.WithLocation(e.GetLocation()),
				endpoint.WithID(e.GetNodeId()),
				endpoint.WithLoadFactor(e.GetLoadFactor()),
				endpoint.WithLocalDC(e.GetLocation() == location),
				endpoint.WithServices(e.GetService()),
			))
		}
	}

	return endpoints, nil
}

func (c *Client) WhoAmI(ctx context.Context) (whoAmI *discovery.WhoAmI, err error) {
	var (
		onDone             = trace.DiscoveryOnWhoAmI(c.config.Trace(), &ctx)
		request            = Ydb_Discovery.WhoAmIRequest{}
		response           *Ydb_Discovery.WhoAmIResponse
		whoAmIResultResult Ydb_Discovery.WhoAmIResult
	)
	defer func() {
		if err != nil {
			onDone("", nil, err)
		} else {
			onDone(whoAmI.User, whoAmI.Groups, err)
		}
	}()

	ctx, err = c.config.Meta().Context(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	client, cc, err := c.client(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	defer func() {
		_ = cc.Close()
	}()

	response, err = client.WhoAmI(ctx, &request)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	err = response.GetOperation().GetResult().UnmarshalTo(&whoAmIResultResult)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return &discovery.WhoAmI{
		User:   whoAmIResultResult.GetUser(),
		Groups: whoAmIResultResult.GetGroups(),
	}, nil
}

func (c *Client) Close(ctx context.Context) error {
	return nil
}
