package balancer

import (
	"context"
	"fmt"
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
	config            balancerConfig.Config
	discoveryClient   discoveryClient
	discoveryRepeater repeater.Repeater
	localDCDetector   func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error)

	conns  xsync.Map[string, *xsync.Once[conn.Conn]]
	banned xsync.Map[string, struct{}]

	state atomic.Pointer[connectionsState]

	onApplyDiscoveredEndpointsMtx xsync.RWMutex
	onApplyDiscoveredEndpoints    []func(ctx context.Context, endpoints []endpoint.Info)
}

func (b *Balancer) OnUpdate(onApplyDiscoveredEndpoints func(ctx context.Context, endpoints []endpoint.Info)) {
	b.onApplyDiscoveredEndpointsMtx.RLock()
	defer b.onApplyDiscoveredEndpointsMtx.RUnlock()

	b.onApplyDiscoveredEndpoints = append(b.onApplyDiscoveredEndpoints, onApplyDiscoveredEndpoints)
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

func (b *Balancer) clusterDiscoveryAttempt(ctx context.Context) (err error) {
	var (
		address = "ydb:///" + b.driverConfig.Endpoint()
		onDone  = trace.DriverOnBalancerClusterDiscoveryAttempt(
			b.driverConfig.Trace(), &ctx,
			stack.FunctionID(
				"github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).clusterDiscoveryAttempt"),
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

	if b.config.DetectLocalDC {
		localDC, err = b.localDCDetector(ctx, endpoints)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
	}

	b.applyDiscoveredEndpoints(ctx, endpoints, localDC)

	return nil
}

func s2s[T1, T2 any](in []T1, f func(T1) T2) (out []T2) {
	out = make([]T2, len(in))
	for i := range in {
		out[i] = f(in[i])
	}

	return out
}

func (b *Balancer) applyDiscoveredEndpoints(ctx context.Context, endpoints []endpoint.Endpoint, localDC string) {
	var (
		onDone = trace.DriverOnBalancerUpdate(
			b.driverConfig.Trace(), &ctx,
			stack.FunctionID(
				"github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).applyDiscoveredEndpoints"),
			b.config.DetectLocalDC,
		)
	)
	state := newConnectionsState(
		endpoints,
		b.config.Filter,
		balancerConfig.Info{SelfLocation: localDC},
		b.config.AllowFallback,
		func(e endpoint.Endpoint) bool {
			return !b.banned.Has(e.Address())
		},
	)

	_, added, dropped := endpoint.Diff(endpoints, b.state.Swap(state).All())

	for _, endpoint := range dropped {
		if cc, ok := b.conns.LoadAndDelete(endpoint.Address()); ok {
			_ = cc.Close(ctx)
		}
	}

	for _, endpoint := range added {
		b.conns.Store(endpoint.Address(), xsync.OnceValue[conn.Conn](func() (conn.Conn, error) {
			cc, err := conn.New(endpoint, b.driverConfig,
				conn.WithOnTransportError(func(ctx context.Context, cc conn.Conn, cause error) {
					if xerrors.IsTransportError(cause,
						//grpcCodes.OK,
						//grpcCodes.ResourceExhausted,
						//grpcCodes.Unavailable,
						grpcCodes.Canceled,
						grpcCodes.Unknown,
						grpcCodes.InvalidArgument,
						grpcCodes.DeadlineExceeded,
						grpcCodes.NotFound,
						grpcCodes.AlreadyExists,
						grpcCodes.PermissionDenied,
						grpcCodes.FailedPrecondition,
						grpcCodes.Aborted,
						grpcCodes.OutOfRange,
						grpcCodes.Unimplemented,
						grpcCodes.Internal,
						grpcCodes.DataLoss,
						grpcCodes.Unauthenticated,
					) {
						b.banned.Store(cc.Endpoint().Address(), struct{}{})
					}
				}),
			)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return cc, nil
		}))
	}

	b.banned.Clear()

	infos := s2s(endpoints, func(e endpoint.Endpoint) endpoint.Info { return e })

	b.onApplyDiscoveredEndpointsMtx.WithRLock(func() {
		for _, onApplyDiscoveredEndpoints := range b.onApplyDiscoveredEndpoints {
			onApplyDiscoveredEndpoints(ctx, infos)
		}
	})

	onDone(
		s2s(endpoints, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
		s2s(added, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
		s2s(dropped, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
		localDC,
	)
}

func (b *Balancer) Close(ctx context.Context) (err error) {
	onDone := trace.DriverOnBalancerClose(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).Close"),
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

	b = &Balancer{
		driverConfig:    driverConfig,
		localDCDetector: detectLocalDC,
	}
	cc, err := conn.New(endpoint.New(driverConfig.Endpoint()), driverConfig)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	b.discoveryClient = internalDiscovery.New(ctx, cc, discoveryConfig)

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
		if d := discoveryConfig.Interval(); d > 0 {
			b.discoveryRepeater = repeater.New(xcontext.ValueOnly(ctx),
				d, b.clusterDiscoveryAttempt,
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

func (b *Balancer) getConn(ctx context.Context) (c conn.Conn, err error) {
	onDone := trace.DriverOnBalancerChooseEndpoint(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).getConn"),
	)
	defer func() {
		if c != nil {
			onDone(c.Endpoint(), err)
		} else {
			onDone(nil, err)
		}
	}()

	if err = ctx.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	state := b.state.Load()

	defer func() {
		if err != nil || (len(state.all)*2 < len(b.state.Load().all) && b.discoveryRepeater != nil) {
			b.discoveryRepeater.Force()
		}
	}()

	for i := 0; ; i++ {
		e := state.Next(ctx)
		if e == nil {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("%w: cannot get connection from Balancer after %d attempts", ErrNoEndpoints, i+1),
			)
		}
		cc := b.conns.Must(e.Address())
		c, err = cc.Get()
		if err == nil {
			return c, nil
		}
		b.banned.Store(e.Address(), struct{}{})
		state = state.exclude(e)
	}
}
