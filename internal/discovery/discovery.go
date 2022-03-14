package discovery

import (
	"context"
	"net"
	"strconv"

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
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func New(
	ctx context.Context,
	cc conn.Conn,
	crudExplorer cluster.CRUDExplorerLocker,
	driverTrace trace.Driver,
	opts ...config.Option,
) (_ discovery.Client, err error) {
	c := &client{
		cc:      cc,
		config:  config.New(opts...),
		service: Ydb_Discovery_V1.NewDiscoveryServiceClient(cc),
	}

	if c.config.Interval() <= 0 {
		_ = crudExplorer.Insert(ctx, cc.Endpoint())
		return c, nil
	}

	var curr, next []endpoint.Endpoint

	curr, err = c.Discover(ctx)
	if err != nil {
		return nil, err
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
					return err
				}

				// NOTE: curr endpoints must be sorted here.
				cluster.SortEndpoints(next)

				crudExplorer.Lock()
				defer crudExplorer.Unlock()

				cluster.DiffEndpoints(curr, next,
					func(i, j int) {
						// Endpoints are equal, but we still need to update meta
						// data such that load factor and others.
						crudExplorer.Update(
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

type client struct {
	config  config.Config
	service Ydb_Discovery_V1.DiscoveryServiceClient
	cc      conn.Conn
}

func (d *client) Discover(ctx context.Context) (endpoints []endpoint.Endpoint, err error) {
	onDone := trace.DiscoveryOnDiscover(d.config.Trace(), &ctx, d.config.Endpoint(), d.config.Database())

	var location string
	defer func() {
		nodes := make([]trace.EndpointInfo, 0, len(endpoints))
		for _, e := range endpoints {
			nodes = append(nodes, e.Copy())
		}
		onDone(location, nodes, err)
	}()

	request := Ydb_Discovery.ListEndpointsRequest{
		Database: d.config.Database(),
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
		if e.Ssl == d.config.Secure() {
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

func (d *client) WhoAmI(ctx context.Context) (whoAmI *discovery.WhoAmI, err error) {
	onDone := trace.DiscoveryOnWhoAmI(d.config.Trace(), &ctx)
	defer func() {
		if err != nil {
			onDone("", nil, err)
		} else {
			onDone(whoAmI.User, whoAmI.Groups, err)
		}
	}()
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
	return &discovery.WhoAmI{
		User:   whoAmIResultResult.GetUser(),
		Groups: whoAmIResultResult.GetGroups(),
	}, nil
}

func (d *client) Close(ctx context.Context) error {
	d.cc.Release(ctx)
	return nil
}
