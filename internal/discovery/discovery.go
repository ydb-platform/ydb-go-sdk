package discovery

import (
	"context"
	"io"
	"net"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func New(ctx context.Context, cc grpc.ClientConnInterface, config *config.Config) (*Client, error) {
	return &Client{
		config: config,
		cc:     cc,
		client: Ydb_Discovery_V1.NewDiscoveryServiceClient(cc),
	}, nil
}

var _ discovery.Client = &Client{}

type Client struct {
	config *config.Config
	cc     grpc.ClientConnInterface
	client Ydb_Discovery_V1.DiscoveryServiceClient
}

// Discover cluster endpoints
func (c *Client) Discover(ctx context.Context) (endpoints []endpoint.Endpoint, err error) {
	var (
		onDone = trace.DiscoveryOnDiscover(
			c.config.Trace(), &ctx,
			stack.FunctionID(""),
			c.config.Endpoint(), c.config.Database(),
		)
		request = Ydb_Discovery.ListEndpointsRequest{
			Database: c.config.Database(),
		}
		response *Ydb_Discovery.ListEndpointsResponse
		result   Ydb_Discovery.ListEndpointsResult
		location string
	)
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

	response, err = c.client.ListEndpoints(ctx, &request)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if response.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS {
		return nil, xerrors.WithStackTrace(
			xerrors.Operation(
				xerrors.FromOperation(response.GetOperation()),
			),
		)
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
		onDone             = trace.DiscoveryOnWhoAmI(c.config.Trace(), &ctx, stack.FunctionID(""))
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

	response, err = c.client.WhoAmI(ctx, &request)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if response.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS {
		return nil, xerrors.WithStackTrace(
			xerrors.Operation(
				xerrors.FromOperation(
					response.GetOperation(),
				),
			),
		)
	}

	result := response.GetOperation().GetResult()
	if result == nil {
		return &discovery.WhoAmI{}, nil
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

func (c *Client) Close(context.Context) error {
	if cc, has := c.cc.(io.Closer); has {
		return cc.Close()
	}
	return nil
}
