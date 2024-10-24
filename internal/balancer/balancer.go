package balancer

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	internalDiscovery "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var ErrNoEndpoints = xerrors.Wrap(fmt.Errorf("no endpoints"))

type Balancer struct {
	driverConfig      *config.Config
	config            balancerConfig.Config
	pool              *conn.Pool
	discoveryRepeater repeater.Repeater
	localDCDetector   func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error)

	connectionsState atomic.Pointer[connectionsState]

	mu                         xsync.RWMutex
	onApplyDiscoveredEndpoints []func(ctx context.Context, endpoints []endpoint.Info)
	discoveryConfig            *discoveryConfig.Config
}

func (b *Balancer) OnUpdate(onApplyDiscoveredEndpoints func(ctx context.Context, endpoints []endpoint.Info)) {
	b.mu.WithLock(func() {
		b.onApplyDiscoveredEndpoints = append(b.onApplyDiscoveredEndpoints, onApplyDiscoveredEndpoints)
	})
}

func (b *Balancer) clusterDiscovery(ctx context.Context) (err error) {
	return retry.Retry(
		repeater.WithEvent(ctx, repeater.EventInit),
		func(childCtx context.Context) (err error) {
			if err = b.clusterDiscoveryAttempt(childCtx); err != nil {
				if credentials.IsAccessError(err) {
					return credentials.AccessError("cluster discovery failed", err,
						credentials.WithEndpoint(b.driverConfig.Endpoint()),
						credentials.WithDatabase(b.driverConfig.Database()),
						credentials.WithCredentials(b.driverConfig.Credentials()),
					)
				}
				// if got err but parent context is not done - mark error as retryable
				if ctx.Err() == nil && xerrors.IsTimeoutError(err) {
					return xerrors.WithStackTrace(xerrors.Retryable(err))
				}

				return xerrors.WithStackTrace(err)
			}

			return nil
		},
		retry.WithIdempotent(true),
		retry.WithTrace(b.driverConfig.TraceRetry()),
		retry.WithBudget(b.driverConfig.RetryBudget()),
	)
}

type discoveryAttemptSettings struct {
	address     string
	dialTimeout time.Duration
	client      interface {
		closer.Closer

		Discover(ctx context.Context) ([]endpoint.Endpoint, error)
	}
}

func (b *Balancer) clusterDiscoveryAttempt(ctx context.Context, opts ...func(*discoveryAttemptSettings)) (err error) {
	settings := discoveryAttemptSettings{
		address:     "dns:///" + b.driverConfig.Endpoint(),
		dialTimeout: b.driverConfig.DialTimeout(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&settings)
		}
	}

	onDone := trace.DriverOnBalancerClusterDiscoveryAttempt(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID(
			"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).clusterDiscoveryAttempt",
		),
		settings.address,
		b.driverConfig.Database(),
	)
	defer func() {
		onDone(err)
	}()

	if settings.dialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = xcontext.WithTimeout(ctx, settings.dialTimeout)
		defer cancel()
	}

	if settings.client == nil {
		cc, err := grpc.DialContext(ctx, settings.address, b.driverConfig.GrpcDialOptions()...)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		defer func() {
			_ = cc.Close()
		}()

		settings.client = internalDiscovery.New(ctx, cc, b.discoveryConfig)
		defer func() {
			_ = settings.client.Close(ctx)
		}()
	}

	endpoints, err := settings.client.Discover(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	if b.config.DetectNearestDC {
		localDC, err := b.localDCDetector(ctx, endpoints)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		b.applyDiscoveredEndpoints(ctx, endpoints, localDC)
	} else {
		b.applyDiscoveredEndpoints(ctx, endpoints, "")
	}

	return nil
}

func (b *Balancer) applyDiscoveredEndpoints(ctx context.Context, newest []endpoint.Endpoint, localDC string) {
	var (
		onDone = trace.DriverOnBalancerUpdate(
			b.driverConfig.Trace(), &ctx,
			stack.FunctionID(
				"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).applyDiscoveredEndpoints"),
			b.config.DetectNearestDC,
			b.driverConfig.Database(),
		)
		previous = b.connections().All()
	)
	defer func() {
		_, added, dropped := xslices.Diff(previous, newest, func(lhs, rhs endpoint.Endpoint) int {
			return strings.Compare(lhs.Address(), rhs.Address())
		})
		onDone(
			xslices.Transform(newest, func(t endpoint.Endpoint) trace.EndpointInfo { return t }),
			xslices.Transform(added, func(t endpoint.Endpoint) trace.EndpointInfo { return t }),
			xslices.Transform(dropped, func(t endpoint.Endpoint) trace.EndpointInfo { return t }),
			localDC,
		)
	}()

	connections := endpointsToConnections(b.pool, newest)
	for _, c := range connections {
		b.pool.Allow(ctx, c)
		c.Endpoint().Touch()
	}

	info := balancerConfig.Info{SelfLocation: localDC}
	state := newConnectionsState(connections, b.config.Filter, info, b.config.AllowFallback)

	endpointsInfo := make([]endpoint.Info, len(newest))
	for i, e := range newest {
		endpointsInfo[i] = e
	}

	b.connectionsState.Store(state)

	b.mu.WithLock(func() {
		for _, onApplyDiscoveredEndpoints := range b.onApplyDiscoveredEndpoints {
			onApplyDiscoveredEndpoints(ctx, endpointsInfo)
		}
	})
}

func (b *Balancer) Close(ctx context.Context) (err error) {
	onDone := trace.DriverOnBalancerClose(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).Close"),
	)
	defer func() {
		onDone(err)
	}()

	if b.discoveryRepeater != nil {
		b.discoveryRepeater.Stop()
	}

	return nil
}

func New(
	ctx context.Context,
	driverConfig *config.Config,
	pool *conn.Pool,
	opts ...discoveryConfig.Option,
) (b *Balancer, finalErr error) {
	onDone := trace.DriverOnBalancerInit(
		driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.New"),
		driverConfig.Balancer().String(),
	)
	defer func() {
		onDone(finalErr)
	}()

	b = &Balancer{
		driverConfig: driverConfig,
		discoveryConfig: discoveryConfig.New(append(opts,
			discoveryConfig.With(driverConfig.Common),
			discoveryConfig.WithEndpoint(driverConfig.Endpoint()),
			discoveryConfig.WithDatabase(driverConfig.Database()),
			discoveryConfig.WithSecure(driverConfig.Secure()),
			discoveryConfig.WithMeta(driverConfig.Meta()),
		)...),
		pool:            pool,
		localDCDetector: detectLocalDC,
	}

	if config := driverConfig.Balancer(); config == nil {
		b.config = balancerConfig.Config{}
	} else {
		b.config = *config
	}

	if b.config.SingleConn {
		b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{
			endpoint.New(driverConfig.Endpoint()),
		}, "")
	} else {
		// initialization of balancer state
		if err := b.clusterDiscovery(ctx); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		// run background discovering
		if d := b.discoveryConfig.Interval(); d > 0 {
			b.discoveryRepeater = repeater.New(xcontext.ValueOnly(ctx), d,
				func(ctx context.Context) (err error) {
					return b.clusterDiscoveryAttempt(ctx)
				},
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
		} else if conn.IsBadConn(err, b.driverConfig.ExcludeGRPCCodesForPessimization()...) {
			b.pool.Ban(ctx, cc, err)
		}
	}()

	if ctx, err = b.driverConfig.Meta().Context(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	if err = f(ctx, cc); err != nil {
		if conn.UseWrapping(ctx) {
			if credentials.IsAccessError(err) {
				err = credentials.AccessError("no access", err,
					credentials.WithAddress(cc.Endpoint().String()),
					credentials.WithNodeID(cc.Endpoint().NodeID()),
					credentials.WithCredentials(b.driverConfig.Credentials()),
				)
			}

			return xerrors.WithStackTrace(err)
		}

		return err
	}

	return nil
}

func (b *Balancer) connections() *connectionsState {
	return b.connectionsState.Load()
}

func (b *Balancer) getConn(ctx context.Context) (c conn.Conn, err error) {
	onDone := trace.DriverOnBalancerChooseEndpoint(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).getConn"),
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
