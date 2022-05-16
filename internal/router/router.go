package router

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	discoveryBuilder "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var ErrClusterEmpty = xerrors.Wrap(fmt.Errorf("cluster empty"))

type router struct {
	config            config.Config
	balancerConfig    balancer.Config
	pool              *conn.Pool
	discovery         discovery.Client
	discoveryRepeater repeater.Repeater

	m                sync.RWMutex
	connectionsState *connectionsState
}

func (r *router) clusterDiscovery(ctx context.Context) error {
	endpoints, err := r.discovery.Discover(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	r.applyDiscoveredEndpoints(ctx, endpoints)
	return nil
}

func (r *router) applyDiscoveredEndpoints(ctx context.Context, endpoints []endpoint.Endpoint) {
	connections := endpointsToConnections(r.pool, endpoints)
	for _, c := range connections {
		r.pool.Allow(ctx, c)
	}

	state := newConnectionsState(connections, r.balancerConfig.IsPreferConn, r.balancerConfig.AllowFalback)

	r.m.Lock()
	defer r.m.Unlock()
	r.connectionsState = state
}

func (r *router) Discovery() discovery.Client {
	return r.discovery
}

func (r *router) Close(ctx context.Context) (err error) {
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
	onDone := trace.DriverOnInit(
		c.Trace(),
		&ctx,
		c.Endpoint(),
		c.Database(),
		c.Secure(),
	)
	defer func() {
		onDone(err)
	}()

	r := &router{
		config: c,
		pool:   pool,
	}

	if balancerConfig := c.Balancer(); balancerConfig == nil {
		r.balancerConfig = balancer.Config{}
	} else {
		r.balancerConfig = *balancerConfig
	}

	discoveryEndpoint := endpoint.New(c.Endpoint())
	discoveryConnection := pool.Get(discoveryEndpoint)

	discoveryConfig := discoveryConfig.New(opts...)

	r.discovery = discoveryBuilder.New(
		discoveryConnection,
		discoveryConfig,
	)

	if r.balancerConfig.SingleConn {
		r.connectionsState = newConnectionsState(
			endpointsToConnections(pool, []endpoint.Endpoint{discoveryEndpoint}),
			nil, false)
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
				repeater.WithTrace(r.config.Trace()),
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
	return r.config.Endpoint()
}

func (r *router) Name() string {
	return r.config.Database()
}

func (r *router) Secure() bool {
	return r.config.Secure()
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
			if xerrors.MustPessimizeEndpoint(err, r.config.ExcludeGRPCCodesForPessimization()...) {
				r.pool.Ban(ctx, cc, err)
			}
		}
	}()

	if ctx, err = r.config.Meta().Meta(ctx); err != nil {
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

func (r *router) getConn(ctx context.Context) (conn.Conn, error) {
	state := r.connections()
	c, failedCount := state.Connection(ctx)
	if failedCount*2 > state.PreferCount {
		r.discoveryRepeater.Force()
	}

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
