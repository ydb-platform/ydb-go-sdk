package balancer

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	internalDiscovery "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var ErrNoEndpoints = xerrors.Wrap(fmt.Errorf("no endpoints"))

type discoveryClient interface {
	closer.Closer

	Discover(ctx context.Context) ([]endpoint.Endpoint, error)
}

type Balancer struct {
	driverConfig      *config.Config
	balancerConfig    balancerConfig.Config
	pool              *conn.Pool
	discoveryClient   discoveryClient
	discoveryRepeater repeater.Repeater
	localDCDetector   func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error)

	mu               xsync.RWMutex
	connectionsState *connectionsState

	onApplyDiscoveredEndpoints []func(ctx context.Context, endpoints []endpoint.Info)
}

func (b *Balancer) HasNode(id uint32) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, has := b.connectionsState.connByNodeID[id]; has {
		return true
	}
	return false
}

func (b *Balancer) OnUpdate(onApplyDiscoveredEndpoints func(ctx context.Context, endpoints []endpoint.Info)) {
	b.mu.WithLock(func() {
		b.onApplyDiscoveredEndpoints = append(b.onApplyDiscoveredEndpoints, onApplyDiscoveredEndpoints)
	})
}

func (b *Balancer) clusterDiscovery(ctx context.Context) (err error) {
	if err = retry.Retry(ctx, func(childCtx context.Context) (err error) {
		if err = b.clusterDiscoveryAttempt(childCtx); err != nil {
			if xerrors.IsTransportError(err, grpcCodes.Unauthenticated) {
				return xerrors.WithStackTrace(err)
			}
			// if got err but parent context is not done - mark error as retryable
			if ctx.Err() == nil && xerrors.IsTimeoutError(err) {
				return xerrors.WithStackTrace(xerrors.Retryable(err))
			}
			return xerrors.WithStackTrace(err)
		}
		return nil
	}, retry.WithIdempotent(true)); err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

func (b *Balancer) clusterDiscoveryAttempt(ctx context.Context) (err error) {
	var (
		address = "ydb:///" + b.driverConfig.Endpoint()
		onDone  = trace.DriverOnBalancerClusterDiscoveryAttempt(
			b.driverConfig.Trace(),
			&ctx,
			address,
		)
		endpoints []endpoint.Endpoint
		localDC   string
		cancel    context.CancelFunc
	)
	defer func() {
		onDone(err)
	}()

	if dialTimeout := b.driverConfig.DialTimeout(); dialTimeout > 0 {
		ctx, cancel = xcontext.WithTimeout(ctx, dialTimeout)
	} else {
		ctx, cancel = xcontext.WithCancel(ctx)
	}
	defer cancel()

	endpoints, err = b.discoveryClient.Discover(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	if b.balancerConfig.DetectlocalDC {
		localDC, err = b.localDCDetector(ctx, endpoints)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
	}

	b.applyDiscoveredEndpoints(ctx, endpoints, localDC)

	return nil
}

func (b *Balancer) applyDiscoveredEndpoints(ctx context.Context, endpoints []endpoint.Endpoint, localDC string) {
	onDone := trace.DriverOnBalancerUpdate(
		b.driverConfig.Trace(),
		&ctx,
		b.balancerConfig.DetectlocalDC,
	)
	defer func() {
		nodes := make([]trace.EndpointInfo, 0, len(endpoints))
		for _, e := range endpoints {
			nodes = append(nodes, e.Copy())
		}
		onDone(nodes, localDC, nil)
	}()

	connections := endpointsToConnections(b.pool, endpoints)
	for _, c := range connections {
		b.pool.Allow(ctx, c)
		c.Endpoint().Touch()
	}

	info := balancerConfig.Info{SelfLocation: localDC}
	state := newConnectionsState(connections, b.balancerConfig.IsPreferConn, info, b.balancerConfig.AllowFalback)

	endpointsInfo := make([]endpoint.Info, len(endpoints))
	for i, e := range endpoints {
		endpointsInfo[i] = e
	}

	b.mu.WithLock(func() {
		b.connectionsState = state
		for _, onApplyDiscoveredEndpoints := range b.onApplyDiscoveredEndpoints {
			onApplyDiscoveredEndpoints(ctx, endpointsInfo)
		}
	})
}

func (b *Balancer) Close(ctx context.Context) (err error) {
	onDone := trace.DriverOnBalancerClose(
		b.driverConfig.Trace(),
		&ctx,
	)
	defer func() {
		onDone(err)
	}()

	if b.discoveryRepeater != nil {
		b.discoveryRepeater.Stop()
	}

	if err = b.discoveryClient.Close(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func New(
	ctx context.Context,
	driverConfig *config.Config,
	pool *conn.Pool,
	opts ...discoveryConfig.Option,
) (b *Balancer, err error) {
	var (
		onDone = trace.DriverOnBalancerInit(
			driverConfig.Trace(),
			&ctx,
		)
		discoveryConfig = discoveryConfig.New(append(opts,
			discoveryConfig.With(driverConfig.Common),
			discoveryConfig.WithEndpoint(driverConfig.Endpoint()),
			discoveryConfig.WithDatabase(driverConfig.Database()),
			discoveryConfig.WithSecure(driverConfig.Secure()),
			discoveryConfig.WithMeta(driverConfig.Meta()),
		)...)
	)
	defer func() {
		onDone(err)
	}()

	b = &Balancer{
		driverConfig:    driverConfig,
		pool:            pool,
		localDCDetector: detectLocalDC,
		discoveryClient: internalDiscovery.New(
			pool.Get(
				endpoint.New(driverConfig.Endpoint()),
			),
			discoveryConfig,
		),
	}

	if config := driverConfig.Balancer(); config == nil {
		b.balancerConfig = balancerConfig.Config{}
	} else {
		b.balancerConfig = *config
	}

	if b.balancerConfig.SingleConn {
		b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{
			endpoint.New(driverConfig.Endpoint()),
		}, "")
	} else {
		// initialization of balancer state
		if err = b.clusterDiscovery(ctx); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		// run background discovering
		if d := discoveryConfig.Interval(); d > 0 {
			b.discoveryRepeater = repeater.New(d, b.clusterDiscoveryAttempt,
				repeater.WithName("discovery"),
				repeater.WithTrace(b.driverConfig.Trace()),
			)
		}
	}

	return b, nil
}

func (b *Balancer) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) error {
	return b.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
		return cc.Invoke(ctx, method, args, reply, opts...)
	})
}

func (b *Balancer) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, err error) {
	var client grpc.ClientStream
	err = b.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
		client, err = cc.NewStream(ctx, desc, method, opts...)
		return err
	})
	if err == nil {
		return client, nil
	}
	return nil, err
}

func (b *Balancer) wrapCall(ctx context.Context, f func(ctx context.Context, cc conn.Conn) error) (err error) {
	cc, err := b.getConn(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	defer func() {
		if err == nil {
			if cc.GetState() == conn.Banned {
				b.pool.Allow(ctx, cc)
			}
		} else if xerrors.MustPessimizeEndpoint(err, b.driverConfig.ExcludeGRPCCodesForPessimization()...) {
			b.pool.Ban(ctx, cc, err)
		}
	}()

	if ctx, err = b.driverConfig.Meta().Context(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	if err = f(ctx, cc); err != nil {
		if conn.UseWrapping(ctx) {
			return xerrors.WithStackTrace(err)
		}
		return err
	}

	return nil
}

func (b *Balancer) connections() *connectionsState {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.connectionsState
}

func (b *Balancer) getConn(ctx context.Context) (c conn.Conn, err error) {
	onDone := trace.DriverOnBalancerChooseEndpoint(
		b.driverConfig.Trace(),
		&ctx,
	)
	defer func() {
		if err == nil {
			onDone(c.Endpoint(), nil)
		} else {
			onDone(nil, err)
		}
	}()

	if err = ctx.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	var (
		state       = b.connections()
		failedCount int
	)

	defer func() {
		if failedCount*2 > state.PreferredCount() && b.discoveryRepeater != nil {
			b.discoveryRepeater.Force()
		}
	}()

	c, failedCount = state.GetConnection(ctx)
	if c == nil {
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("%w: cannot get connection from Balancer after %d attempts", ErrNoEndpoints, failedCount),
		)
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
