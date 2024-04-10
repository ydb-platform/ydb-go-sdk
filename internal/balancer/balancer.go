package balancer

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"

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
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var ErrNoEndpoints = xerrors.Wrap(fmt.Errorf("no endpoints"))

type (
	discoveryClient interface {
		closer.Closer

		Discover(ctx context.Context) ([]endpoint.Info, error)
	}
	Balancer struct {
		config            *balancerConfig.Config
		pool              connPool
		discoveryClient   discoveryClient
		discoveryRepeater repeater.Repeater
		localDCDetector   func(ctx context.Context, endpoints []endpoint.Info) (string, error)

		connChilds map[string]*xcontext.CancelsGuard

		connections atomic.Pointer[connections[conn.Conn]]

		closed chan struct{}

		onApplyDiscoveredEndpoints []func(ctx context.Context, endpoints []endpoint.Info)
	}
)

func (b *Balancer) clusterDiscovery(ctx context.Context, opts ...retry.Option) (err error) {
	return retry.Retry(repeater.WithEvent(ctx, repeater.EventInit),
		func(childCtx context.Context) (err error) {
			if err = b.clusterDiscoveryAttempt(childCtx); err != nil {
				if credentials.IsAccessError(err) {
					return credentials.AccessError("cluster discovery failed", err,
						credentials.WithEndpoint(b.config.Endpoint()),
						credentials.WithDatabase(b.config.Database()),
						credentials.WithCredentials(b.config.Credentials()),
					)
				}
				// if got err but parent context is not done - mark error as retryable
				if ctx.Err() == nil && xerrors.IsTimeoutError(err) {
					return xerrors.WithStackTrace(xerrors.Retryable(err))
				}

				return xerrors.WithStackTrace(err)
			}

			return nil
		}, opts...,
	)
}

func (b *Balancer) clusterDiscoveryAttempt(ctx context.Context) (err error) {
	var (
		address = "ydb:///" + b.config.Endpoint()
		onDone  = trace.DriverOnBalancerClusterDiscoveryAttempt(
			b.config.Trace(), &ctx,
			stack.FunctionID(
				"github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).clusterDiscoveryAttempt"),
			address,
		)
		endpoints []endpoint.Info
		localDC   string
		cancel    context.CancelFunc
	)
	defer func() {
		onDone(err)
	}()

	if dialTimeout := b.config.DialTimeout(); dialTimeout > 0 {
		ctx, cancel = xcontext.WithTimeout(ctx, dialTimeout)
	} else {
		ctx, cancel = xcontext.WithCancel(ctx)
	}
	defer cancel()

	endpoints, err = b.discoveryClient.Discover(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	if b.config.DetectLocalDC() {
		localDC, err = b.localDCDetector(ctx, endpoints)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
	}

	b.applyDiscoveredEndpoints(ctx, endpoints, localDC)

	return nil
}

func endpointsDiff[T endpoint.Info](newest, previous []T) (nodes, added, dropped []T) {
	nodes = make([]T, 0, len(newest))
	added = make([]T, 0, len(previous))
	dropped = make([]T, 0, len(previous))
	var (
		newestMap   = make(map[string]struct{}, len(newest))
		previousMap = make(map[string]struct{}, len(previous))
	)
	sort.Slice(newest, func(i, j int) bool {
		return newest[i].Address() < newest[j].Address()
	})
	sort.Slice(previous, func(i, j int) bool {
		return previous[i].Address() < previous[j].Address()
	})
	for _, c := range previous {
		previousMap[c.Address()] = struct{}{}
	}
	for _, c := range newest {
		nodes = append(nodes, c)
		newestMap[c.Address()] = struct{}{}
		if _, has := previousMap[c.Address()]; !has {
			added = append(added, c)
		}
	}
	for _, c := range previous {
		if _, has := newestMap[c.Address()]; !has {
			dropped = append(dropped, c)
		}
	}

	return nodes, added, dropped
}

func toTraceEndpointInfo[T endpoint.Info](in []T) (out []trace.EndpointInfo) {
	out = make([]trace.EndpointInfo, 0, len(in))
	for _, e := range in {
		out = append(out, e)
	}

	return out
}

func (b *Balancer) applyDiscoveredEndpoints(ctx context.Context, endpoints []endpoint.Info, localDC string) {
	onDone := trace.DriverOnBalancerUpdate(
		b.config.Trace(), &ctx,
		stack.FunctionID(
			"github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).applyDiscoveredEndpoints"),
		b.config.DetectLocalDC(),
	)

	conns := endpointsToConnections(b.pool, endpoints)

	info := balancerConfig.Info{SelfLocation: localDC}
	newestConnections := newConnections(conns, b.config.Filter, info, b.config.AllowFallback())
	previousConnections := b.connections.Swap(newestConnections)
	defer func() {
		nodes, added, dropped := endpointsDiff(
			newestConnections.all,
			func() []conn.Conn {
				if previousConnections != nil {
					return previousConnections.all
				}

				return nil
			}(),
		)
		for _, e := range dropped {
			b.connChilds[e.Address()].Cancel()
			delete(b.connChilds, e.Address())
		}
		onDone(
			toTraceEndpointInfo(nodes),
			toTraceEndpointInfo(added),
			toTraceEndpointInfo(dropped),
			localDC,
		)
	}()
	for _, onApplyDiscoveredEndpoints := range b.onApplyDiscoveredEndpoints {
		onApplyDiscoveredEndpoints(ctx, endpoints)
	}
}

func (b *Balancer) Close(ctx context.Context) (err error) {
	close(b.closed)

	onDone := trace.DriverOnBalancerClose(
		b.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).Close"),
	)
	defer func() {
		onDone(err)
	}()

	if b.discoveryRepeater != nil {
		b.discoveryRepeater.Stop()
	}

	b.applyDiscoveredEndpoints(ctx, nil, "")

	if err = b.discoveryClient.Close(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (b *Balancer) markConnAsBad(ctx context.Context, cc conn.Conn, cause error) {
	if !xerrors.IsTransportError(cause,
		grpcCodes.ResourceExhausted,
		grpcCodes.Unavailable,
		// grpcCodes.OK,
		// grpcCodes.Canceled,
		// grpcCodes.Unknown,
		// grpcCodes.InvalidArgument,
		// grpcCodes.DeadlineExceeded,
		// grpcCodes.NotFound,
		// grpcCodes.AlreadyExists,
		// grpcCodes.PermissionDenied,
		// grpcCodes.FailedPrecondition,
		// grpcCodes.Aborted,
		// grpcCodes.OutOfRange,
		// grpcCodes.Unimplemented,
		// grpcCodes.Internal,
		// grpcCodes.DataLoss,
		// grpcCodes.Unauthenticated,
	) {
		return
	}

	newestConnections, changed := b.connections.Load().withBadConn(cc)

	if changed {
		onDone := trace.DriverOnBalancerMarkConnAsBad(
			b.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).markConnAsBad"),
			cc, cause,
		)

		b.connections.Store(newestConnections)

		onDone(toTraceEndpointInfo(newestConnections.prefer), toTraceEndpointInfo(newestConnections.fallback))
	}
}

type newBalancerOption func(b *Balancer) error

func newBalancer(
	config *balancerConfig.Config,
	pool connPool,
	opts ...newBalancerOption,
) (b *Balancer, finalErr error) {
	b = &Balancer{
		config:          config,
		pool:            pool,
		localDCDetector: detectLocalDC,
		connChilds:      make(map[string]*xcontext.CancelsGuard),
		closed:          make(chan struct{}),
	}
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
	}

	return b, nil
}

func New(
	ctx context.Context,
	driverConfig *config.Config,
	pool *conn.Pool,
	opts ...discoveryConfig.Option,
) (b *Balancer, finalErr error) {
	var (
		onDone = trace.DriverOnBalancerInit(
			driverConfig.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.New"),
			driverConfig.Balancer().String(),
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
		onDone(finalErr)
	}()

	b, err := newBalancer(driverConfig.Balancer(), pool,
		func(b *Balancer) error {
			b.discoveryClient = internalDiscovery.New(ctx, pool.Get(
				endpoint.New(driverConfig.Endpoint()),
			), discoveryConfig)
			if config := driverConfig.Balancer(); config != nil {
				b.config = config
			}
			if b.config.SingleConn() {
				b.applyDiscoveredEndpoints(ctx, []endpoint.Info{
					endpoint.New(driverConfig.Endpoint()),
				}, "")
			} else {
				// initialization of balancer state
				if err := b.clusterDiscovery(ctx,
					retry.WithIdempotent(true),
					retry.WithTrace(driverConfig.TraceRetry()),
				); err != nil {
					return xerrors.WithStackTrace(err)
				}
				// run background discovering
				if d := discoveryConfig.Interval(); d > 0 {
					b.discoveryRepeater = repeater.New(xcontext.ValueOnly(ctx),
						d, b.clusterDiscoveryAttempt,
						repeater.WithName("discovery"),
						repeater.WithTrace(b.config.Trace()),
					)
				}
			}

			return nil
		},
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
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
	select {
	case <-b.closed:
		return xerrors.WithStackTrace(errBalancerClosed)
	default:
		return b.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
			return cc.Invoke(ctx, method, args, reply, opts...)
		})
	}
}

func (b *Balancer) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, err error) {
	select {
	case <-b.closed:
		return nil, xerrors.WithStackTrace(errBalancerClosed)
	default:
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
}

func (b *Balancer) wrapCall(ctx context.Context, f func(ctx context.Context, cc conn.Conn) error) (finalErr error) {
	cc, err := b.getConn(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	defer func() {
		if finalErr != nil {
			b.markConnAsBad(ctx, cc, err)
		}
	}()

	if ctx, err = b.config.Meta().Context(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	if err = f(ctx, cc); err != nil {
		if conn.UseWrapping(ctx) {
			if credentials.IsAccessError(err) {
				err = credentials.AccessError("no access", err,
					credentials.WithAddress(cc.Address()),
					credentials.WithNodeID(cc.NodeID()),
					credentials.WithCredentials(b.config.Credentials()),
				)
			}

			return xerrors.WithStackTrace(err)
		}

		return err
	}

	return nil
}

func (b *Balancer) getConn(ctx context.Context) (c conn.Conn, err error) {
	onDone := trace.DriverOnBalancerGetConn(
		b.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).getConn"),
	)
	defer func() {
		if err == nil {
			if _, has := b.connChilds[c.Address()]; !has {
				b.connChilds[c.Address()] = xcontext.NewCancelsGuard()
			}
			onDone(c, nil)
		} else {
			onDone(nil, err)
		}
	}()

	if err = ctx.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	var (
		connections = b.connections.Load()
		failedCount int
	)

	defer func() {
		if failedCount*2 > connections.PreferredCount() && b.discoveryRepeater != nil {
			b.discoveryRepeater.Force()
		}
	}()

	c, failedCount = connections.GetConn(ctx)
	if c == nil {
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("cannot get connection from Balancer after %d attempts: %w", failedCount, ErrNoEndpoints),
		)
	}

	return c, nil
}

type connPool interface {
	Get(e endpoint.Info) conn.Conn
}

func endpointsToConnections(p connPool, endpoints []endpoint.Info) []conn.Conn {
	conns := make([]conn.Conn, 0, len(endpoints))
	for _, e := range endpoints {
		conns = append(conns, p.Get(e))
	}

	return conns
}
