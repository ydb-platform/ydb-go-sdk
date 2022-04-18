package discovery

import (
	"context"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"

	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/deadline"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var DefaultDiscoveryInterval = time.Minute

type InserterRemoverExplorerLocker interface {
	cluster.Inserter
	cluster.Remover
	cluster.Explorer
	sync.Locker
}

func New(
	ctx context.Context,
	cc conn.Conn,
	crudExplorer InserterRemoverExplorerLocker,
	driverTrace trace.Driver,
	opts ...config.Option,
) (_ *Client, err error) {
	defer func() {
		if err != nil {
			_ = cc.Release(ctx)
		}
	}()

	c := &Client{
		cc:      cc,
		config:  config.New(opts...),
		service: Ydb_Discovery_V1.NewDiscoveryServiceClient(cc),
	}

	if c.config.Interval() <= 0 {
		crudExplorer.Insert(ctx, cc.Endpoint())
		return c, nil
	}

	var curr, next []endpoint.Endpoint

	curr, err = c.Discover(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	crudExplorer.Lock()
	defer crudExplorer.Unlock()

	// Endpoints must be sorted to merge
	cluster.SortEndpoints(curr)
	for _, e := range curr {
		crudExplorer.Insert(
			ctx,
			e,
			cluster.WithoutLock(),
		)
	}

	crudExplorer.SetExplorer(
		repeater.New(
			deadline.ContextWithoutDeadline(ctx),
			func(ctx context.Context) (err error) {
				next, err = c.Discover(ctx)
				if err != nil {
					return xerrors.WithStackTrace(err)
				}

				// NOTE: curr endpoints must be sorted here.
				cluster.SortEndpoints(next)

				crudExplorer.Lock()
				defer crudExplorer.Unlock()

				cluster.DiffEndpoints(curr, next,
					func(i, j int) {
						crudExplorer.Remove(
							ctx,
							curr[i],
							cluster.WithoutLock(),
						)
						crudExplorer.Insert(
							ctx,
							next[j],
							cluster.WithoutLock(),
						)
					},
					func(i, j int) {
						crudExplorer.Insert(
							ctx,
							next[j],
							cluster.WithoutLock(),
						)
					},
					func(i, j int) {
						crudExplorer.Remove(
							ctx,
							curr[i],
							cluster.WithoutLock(),
						)
					},
				)

				curr = next

				return nil
			},
			repeater.WithInterval(c.config.Interval()),
			repeater.WithName("discovery"),
			repeater.WithTrace(driverTrace),
		),
	)

	return c, nil
}

var _ discovery.Client = &Client{}

type Client struct {
	config  *config.Config
	service Ydb_Discovery_V1.DiscoveryServiceClient
	cc      conn.Conn
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

	ctx, err = c.config.Meta().Meta(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	response, err = c.service.ListEndpoints(ctx, &request)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
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

	ctx, err = c.config.Meta().Meta(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	response, err = c.service.WhoAmI(ctx, &request)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &whoAmIResultResult)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return &discovery.WhoAmI{
		User:   whoAmIResultResult.GetUser(),
		Groups: whoAmIResultResult.GetGroups(),
	}, nil
}

func (c *Client) Close(ctx context.Context) error {
	return c.cc.Release(ctx)
}
