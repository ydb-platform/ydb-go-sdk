package router

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	discoveryBuilder "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	routerconfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/router/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var ErrClusterEmpty = xerrors.Wrap(fmt.Errorf("cluster empty"))

type router struct {
	driverConfig      config.Config
	routerConfig      routerconfig.Config
	pool              *conn.Pool
	discovery         discovery.Client
	discoveryRepeater repeater.Repeater
	localDCDetector   func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error)

	m                sync.RWMutex
	connectionsState *connectionsState
}

func (r *router) clusterDiscovery(ctx context.Context) (err error) {
	var (
		onDone = trace.DriverOnBalancerUpdate(
			r.driverConfig.Trace(),
			&ctx,
			r.routerConfig.DetectlocalDC,
		)
		endpoints []endpoint.Endpoint
		localDC   string
	)

	defer func() {
		nodes := make([]trace.EndpointInfo, 0, len(endpoints))
		for _, e := range endpoints {
			nodes = append(nodes, e.Copy())
		}
		onDone(
			nodes,
			localDC,
			err,
		)
	}()

	endpoints, err = r.discovery.Discover(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	if r.routerConfig.DetectlocalDC {
		localDC, err = r.localDCDetector(ctx, endpoints)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
	}

	r.applyDiscoveredEndpoints(ctx, endpoints, localDC)

	return nil
}

func (r *router) applyDiscoveredEndpoints(ctx context.Context, endpoints []endpoint.Endpoint, localDC string) {
	connections := endpointsToConnections(r.pool, endpoints)
	for _, c := range connections {
		r.pool.Allow(ctx, c)
	}

	routerInfo := routerconfig.Info{SelfLocation: localDC}
	state := newConnectionsState(connections, r.routerConfig.IsPreferConn, routerInfo, r.routerConfig.AllowFalback)

	r.m.Lock()
	defer r.m.Unlock()

	r.connectionsState = state
}

func (r *router) Discovery() discovery.Client {
	return r.discovery
}

func (r *router) Close(ctx context.Context) (err error) {
	onDone := trace.DriverOnBalancerClose(
		r.driverConfig.Trace(),
		&ctx,
	)
	defer func() {
		onDone(err)
	}()

	issues := make([]error, 0, 2)

	if r.discoveryRepeater != nil {
		r.discoveryRepeater.Stop()
	}

	if err = r.discovery.Close(ctx); err != nil {
		issues = append(issues, err)
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("router close failed", issues...))
	}

	return nil
}

func New(
	ctx context.Context,
	c config.Config,
	pool *conn.Pool,
	opts ...discoveryConfig.Option,
) (_ Connection, err error) {
	onDone := trace.DriverOnBalancerInit(
		c.Trace(),
		&ctx,
	)
	defer func() {
		onDone(err)
	}()

	r := &router{
		driverConfig:    c,
		pool:            pool,
		localDCDetector: detectLocalDC,
	}

	if balancerConfig := c.Balancer(); balancerConfig == nil {
		r.routerConfig = routerconfig.Config{}
	} else {
		r.routerConfig = *balancerConfig
	}

	discoveryEndpoint := endpoint.New(c.Endpoint())
	discoveryConnection := pool.Get(discoveryEndpoint)

	discoveryConfig := discoveryConfig.New(opts...)

	r.discovery = discoveryBuilder.New(
		discoveryConnection,
		discoveryConfig,
	)

	if r.routerConfig.SingleConn {
		r.connectionsState = newConnectionsState(
			endpointsToConnections(pool, []endpoint.Endpoint{discoveryEndpoint}),
			nil, routerconfig.Info{}, false)
	} else {
		if err = r.clusterDiscovery(ctx); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		if d := discoveryConfig.Interval(); d > 0 {
			r.discoveryRepeater = repeater.New(d, func(ctx context.Context) (err error) {
				ctx, cancel := context.WithTimeout(ctx, d)
				defer cancel()

				return r.clusterDiscovery(ctx)
			},
				repeater.WithName("discovery"),
				repeater.WithTrace(r.driverConfig.Trace()),
			)
		}
	}

	var cancel context.CancelFunc
	if t := c.DialTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.DialTimeout())
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	return r, nil
}

func (r *router) Endpoint() string {
	return r.driverConfig.Endpoint()
}

func (r *router) Name() string {
	return r.driverConfig.Database()
}

func (r *router) Secure() bool {
	return r.driverConfig.Secure()
}

func (r *router) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) error {
	return r.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
		return cc.Invoke(ctx, method, args, reply, opts...)
	})
}

func (r *router) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, err error) {
	var client grpc.ClientStream
	err = r.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
		client, err = cc.NewStream(ctx, desc, method, opts...)
		return err
	})
	if err == nil {
		return client, nil
	}
	return nil, err
}

func (r *router) wrapCall(ctx context.Context, f func(ctx context.Context, cc conn.Conn) error) (err error) {
	cc, err := r.getConn(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	defer func() {
		if err == nil {
			if cc.GetState() == conn.Banned {
				r.pool.Allow(ctx, cc)
			}
		} else {
			if xerrors.MustPessimizeEndpoint(err, r.driverConfig.ExcludeGRPCCodesForPessimization()...) {
				r.pool.Ban(ctx, cc, err)
			}
		}
	}()

	if ctx, err = r.driverConfig.Meta().Meta(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	if err = f(ctx, cc); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (r *router) connections() *connectionsState {
	r.m.RLock()
	defer r.m.RUnlock()

	return r.connectionsState
}

func (r *router) getConn(ctx context.Context) (c conn.Conn, err error) {
	onDone := trace.DriverOnBalancerChooseEndpoint(
		r.driverConfig.Trace(),
		&ctx,
	)
	defer func() {
		if err == nil {
			onDone(c.Endpoint(), nil)
		} else {
			onDone(nil, err)
		}
	}()

	var (
		state       = r.connections()
		failedCount int
	)

	defer func() {
		if failedCount*2 > state.PreferredCount() {
			r.discoveryRepeater.Force()
		}
	}()

	c, failedCount = state.GetConnection(ctx)
	if c == nil {
		return nil, xerrors.WithStackTrace(ErrClusterEmpty)
	}
	return c, nil
}

func endpointsToConnections(p *conn.Pool, endpoints []endpoint.Endpoint) []conn.Conn {
	conns := make([]conn.Conn, 0, len(endpoints))
	for _, e := range endpoints {
		conns = append(conns, p.Get(e))
	}
	return conns
}
