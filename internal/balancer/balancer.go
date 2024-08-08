package balancer

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/cluster"
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

type discoveryClient interface {
	closer.Closer

	Discover(ctx context.Context) ([]endpoint.Endpoint, error)
}

type Balancer struct {
	driverConfig      *config.Config
	config            balancerConfig.Config
	pool              *conn.Pool
	discoveryClient   discoveryClient
	discoveryRepeater repeater.Repeater

	cluster atomic.Pointer[cluster.Cluster]
	conns   xsync.Map[endpoint.Endpoint, conn.Conn]
	banned  xsync.Set[endpoint.Endpoint]

	localDCDetector func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error)

	mu                         xsync.RWMutex
	onApplyDiscoveredEndpoints []func(ctx context.Context, endpoints []endpoint.Info)
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

func (b *Balancer) applyDiscoveredEndpoints(ctx context.Context, newest []endpoint.Endpoint, localDC string) {
	onDone := trace.DriverOnBalancerUpdate(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID(
			"github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).applyDiscoveredEndpoints"),
		b.config.DetectLocalDC,
	)

	state := cluster.New(newest,
		cluster.From(b.cluster.Load()),
		cluster.WithFallback(b.config.AllowFallback),
		cluster.WithFilter(func(e endpoint.Info) bool {
			if b.config.Filter == nil {
				return true
			}

			return b.config.Filter.Allow(balancerConfig.Info{SelfLocation: localDC}, e)
		}),
	)

	previous := b.cluster.Swap(state)

	_, added, dropped := xslices.Diff(previous.All(), newest, func(lhs, rhs endpoint.Endpoint) int {
		return strings.Compare(lhs.Address(), rhs.Address())
	})

	for _, e := range dropped {
		c, ok := b.conns.Extract(e)
		if !ok {
			panic("wrong balancer state")
		}
		b.pool.Put(ctx, c)
	}

	for _, e := range added {
		cc, err := b.pool.Get(ctx, e)
		if err != nil {
			b.banned.Add(e)
		} else {
			b.conns.Set(e, cc)
		}
	}

	defer func() {
		onDone(
			xslices.Transform(newest, func(t endpoint.Endpoint) trace.EndpointInfo { return t }),
			xslices.Transform(added, func(t endpoint.Endpoint) trace.EndpointInfo { return t }),
			xslices.Transform(dropped, func(t endpoint.Endpoint) trace.EndpointInfo { return t }),
			localDC,
		)
	}()

	endpoints := xslices.Transform(newest, func(e endpoint.Endpoint) endpoint.Info {
		return e
	})

	b.mu.WithLock(func() {
		for _, onApplyDiscoveredEndpoints := range b.onApplyDiscoveredEndpoints {
			onApplyDiscoveredEndpoints(ctx, endpoints)
		}
	})
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

	cc, err := pool.Get(ctx, endpoint.New(driverConfig.Endpoint()))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	b = &Balancer{
		config:          balancerConfig.Config{},
		driverConfig:    driverConfig,
		pool:            pool,
		discoveryClient: internalDiscovery.New(ctx, cc, discoveryConfig),
		localDCDetector: detectLocalDC,
	}

	if config := driverConfig.Balancer(); config != nil {
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

	defer func() {
		if err == nil {
			if cc.GetState() == conn.Banned {
				b.banned.Remove(cc.Endpoint())
			}
		} else if conn.IsBadConn(err, b.driverConfig.ExcludeGRPCCodesForPessimization()...) {
			b.banned.Add(cc.Endpoint())
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

func (b *Balancer) getConn(ctx context.Context) (c conn.Conn, finalErr error) {
	onDone := trace.DriverOnBalancerChooseEndpoint(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/balancer.(*Balancer).getConn"),
	)

	defer func() {
		if finalErr == nil {
			onDone(c.Endpoint(), nil)
		} else {
			if b.cluster.Load().Availability() < 0.5 && b.discoveryRepeater != nil {
				b.discoveryRepeater.Force()
			}

			onDone(nil, finalErr)
		}
	}()

	for attempts := 1; ; attempts++ {
		if err := ctx.Err(); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		state := b.cluster.Load()

		e, err := state.Next(ctx)
		if err != nil {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("%w: cannot get connection from Balancer after %d attempts", cluster.ErrNoEndpoints, attempts),
			)
		}

		cc, err := b.pool.Get(ctx, e)
		if err == nil {
			return cc, nil
		}

		if b.cluster.CompareAndSwap(state, cluster.Without(b.cluster.Load(), e)) {
			b.banned.Add(e)
		}
	}
}
