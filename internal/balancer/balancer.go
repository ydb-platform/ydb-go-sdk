package balancer

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	internalDiscovery "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xresolver"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	ErrNoEndpoints    = xerrors.Wrap(xerrors.Retryable(fmt.Errorf("no endpoints"), xerrors.WithBackoff(backoff.TypeSlow)))
	errBalancerClosed = xerrors.Wrap(fmt.Errorf("internal ydb sdk balancer closed"))
)

// streamWrapper wraps grpc.ClientStream and triggers pool.Ban on RecvMsg/SendMsg/CloseSend
// errors that qualify as bad connection (same logic as wrapCall defer).
type streamWrapper struct {
	grpc.ClientStream

	onErr func(error)
}

func (s *streamWrapper) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err != nil && !xerrors.Is(err, io.EOF) {
		s.onErr(err)
	}

	return err
}

func (s *streamWrapper) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil && !xerrors.Is(err, io.EOF) {
		s.onErr(err)
	}

	return err
}

type Balancer struct {
	driverConfig        *config.Config
	balancer            strategy.Estimator
	compiledPlan        *strategy.Plan
	discoveryConfig     *discoveryConfig.Config
	pool                *conn.Pool
	discoveryController strategy.Controller

	address string
	cc      atomic.Pointer[grpc.ClientConn]

	discover        func(context.Context, *grpc.ClientConn) (endpoints []endpoint.Endpoint, location string, err error)
	localDCDetector func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error)
	rnd             xrand.Rand

	connectionsState atomic.Pointer[connectionsState]

	closeMu sync.Mutex
	closed  bool
}

func (b *Balancer) policy() strategy.Estimator {
	if b.balancer == nil {
		return strategy.RandomChoice()
	}

	return b.balancer
}

func (b *Balancer) plan() strategy.Plan {
	if b.compiledPlan != nil {
		return *b.compiledPlan
	}

	return strategy.Compile(b.policy())
}

func (b *Balancer) clusterDiscovery(ctx context.Context) (err error) {
	return retry.Retry(
		repeater.WithEvent(ctx, repeater.EventInit),
		func(childCtx context.Context) (err error) {
			if err = b.clusterDiscoveryAttemptWithDial(childCtx); err != nil {
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

// discoveryConn returns connection to database endpoint for discovery call
func (b *Balancer) discoveryConn(ctx context.Context) (*grpc.ClientConn, error) {
	if cc := b.cc.Load(); cc != nil {
		if cc.GetState() == connectivity.Ready {
			return cc, nil
		}

		if b.cc.CompareAndSwap(cc, nil) {
			cc.Close()
		}
	}

	if dialTimeout := b.driverConfig.DialTimeout(); dialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = xcontext.WithTimeout(ctx, dialTimeout)
		defer cancel()
	}

	//nolint:staticcheck,nolintlint
	cc, err := grpc.DialContext(ctx, b.address,
		append(
			b.driverConfig.GrpcDialOptions(),
			grpc.WithResolvers(
				xresolver.New("ydb", b.driverConfig.Trace()),
			),
			grpc.WithBlock(), //nolint:staticcheck,nolintlint
		)...,
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("failed to dial %q: %w", b.driverConfig.Endpoint(), err),
		)
	}

	if err := b.tryStoreDiscoveryConn(cc); err != nil {
		_ = cc.Close()

		return nil, xerrors.WithStackTrace(err)
	}

	return cc, nil
}

func (b *Balancer) tryStoreDiscoveryConn(cc *grpc.ClientConn) error {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		return xerrors.WithStackTrace(errBalancerClosed)
	}

	b.cc.Store(cc)

	return nil
}

func (b *Balancer) clusterDiscoveryAttemptWithDial(ctx context.Context) (finalErr error) {
	onDone := gtrace.DriverOnBalancerClusterDiscoveryAttempt(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID(
			"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).clusterDiscoveryAttemptWithDial",
		),
		b.address,
		b.driverConfig.Database(),
	)
	defer func() {
		onDone(finalErr)
	}()

	cc, err := b.discoveryConn(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	if err = b.clusterDiscoveryAttempt(ctx, cc); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (b *Balancer) clusterDiscoveryAttempt(ctx context.Context, cc *grpc.ClientConn) (finalErr error) {
	onDone := gtrace.DriverOnBalancerClusterDiscoveryAttempt(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID(
			"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).clusterDiscoveryAttempt",
		),
		b.address,
		b.driverConfig.Database(),
	)
	defer func() {
		onDone(finalErr)
	}()

	endpoints, location, err := b.discover(ctx, cc)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	resolvedLocation, err := b.plan().ResolveLocation(ctx, endpoints, location, b.localDCDetector)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	b.applyDiscoveredEndpoints(ctx, endpoints, resolvedLocation)

	return nil
}

func nextState(ctx context.Context, pool interface {
	Get(e endpoint.Endpoint) conn.Conn
	Put(ctx context.Context, cc conn.Conn)
}, quarantine []conn.Conn, active []conn.Conn, endpoints []endpoint.Endpoint) (
	newQuarantine []conn.Conn,
	newActive []conn.Conn,
) {
	newActive = xslices.Filter(
		xslices.Transform(endpoints, func(e endpoint.Endpoint) conn.Conn {
			return pool.Get(e)
		}),
		func(cc conn.Conn) bool { return cc != nil },
	)

	for _, cc := range quarantine {
		pool.Put(ctx, cc)
	}

	for _, cc := range newActive {
		cc.Unban(ctx)
	}

	return active, newActive
}

// releaseStateConns releases connections from state
//
// quarantine refs were acquired in discovery round N-1,
// all refs in round N — each Put matches its own Get.
func (b *Balancer) releaseStateConns(ctx context.Context, state *connectionsState) {
	if state == nil {
		return
	}

	for _, c := range state.quarantine {
		b.pool.Put(ctx, c)
	}

	for _, cc := range state.all {
		b.pool.Put(ctx, cc)
	}
}

func (b *Balancer) applyDiscoveredEndpoints(
	ctx context.Context,
	endpoints []endpoint.Endpoint,
	resolvedLocation strategy.ResolvedLocation,
) {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		b.releaseStateConns(ctx, b.connectionsState.Swap(nil))

		return
	}

	var (
		state      = b.connectionsState.Load()
		active     []conn.Conn
		quarantine []conn.Conn
	)

	if state != nil {
		active = state.All()
		quarantine = state.quarantine
	}
	defer b.traceBalancerUpdate(&ctx, active, endpoints, resolvedLocation)()

	info, selected, estimates := b.selectDiscoveredEndpoints(active, endpoints, resolvedLocation)

	quarantine, connections := nextState(ctx, b.pool, quarantine, active, selected)

	b.connectionsState.Store(newConnectionsStateWithEstimates(
		connections, endpoints, estimates, endpointKeySet(selected), quarantine, info.Rand,
	))
}

func (b *Balancer) selectDiscoveredEndpoints(
	active []conn.Conn,
	endpoints []endpoint.Endpoint,
	resolvedLocation strategy.ResolvedLocation,
) (strategy.Info, []endpoint.Endpoint, []strategy.Estimation) {
	if b.rnd == nil {
		b.rnd = xrand.New(xrand.WithLock())
	}
	info := strategy.Info{
		SelfLocation:   resolvedLocation.SelfLocation,
		PreviousActive: previousEndpoints(active),
		Rand:           b.rnd,
	}
	estimates := b.plan().Estimator().Estimate(info, endpoints)
	selected := endpointsForEstimates(endpoints, b.plan().Active(info, estimates))

	return info, selected, estimates
}

func endpointsForEstimates(
	endpoints []endpoint.Endpoint,
	estimates []strategy.Estimation,
) []endpoint.Endpoint {
	byKey := make(map[endpoint.Key]endpoint.Endpoint, len(endpoints))
	for _, candidate := range endpoints {
		byKey[candidate.Key()] = candidate
	}
	result := make([]endpoint.Endpoint, 0, len(estimates))
	for _, estimation := range estimates {
		if candidate := byKey[estimation.Key]; candidate != nil {
			result = append(result, candidate)
		}
	}

	return result
}

func (b *Balancer) traceBalancerUpdate(
	ctx *context.Context,
	active []conn.Conn,
	endpoints []endpoint.Endpoint,
	resolvedLocation strategy.ResolvedLocation,
) func() {
	onDone := gtrace.DriverOnBalancerUpdate(
		b.driverConfig.Trace(), ctx,
		stack.FunctionID(
			"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).traceBalancerUpdate"),
		resolvedLocation.NeedLocalDC,
		b.driverConfig.Database(),
	)

	return func() {
		_, added, dropped := xslices.Diff(xslices.Transform(active, func(cc conn.Conn) endpoint.Endpoint {
			return cc.Endpoint()
		}), endpoints, endpoint.Compare)

		onDone(
			xslices.Transform(endpoints, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
			xslices.Transform(added, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
			xslices.Transform(dropped, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
			resolvedLocation.SelfLocation,
		)
	}
}

func (b *Balancer) Close(ctx context.Context) (err error) {
	b.closeMu.Lock()
	if b.closed {
		b.closeMu.Unlock()

		return xerrors.WithStackTrace(errBalancerClosed)
	}

	b.closed = true

	oldState := b.connectionsState.Swap(nil)

	controller := b.discoveryController
	b.discoveryController = nil

	discoveryCC := b.cc.Swap(nil)

	b.closeMu.Unlock()

	onDone := gtrace.DriverOnBalancerClose(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).Close"),
	)
	defer func() {
		onDone(err)
	}()

	if controller != nil {
		controller.Stop()
	}

	b.releaseStateConns(ctx, oldState)

	if discoveryCC != nil {
		_ = discoveryCC.Close()
	}

	return nil
}

func makeDiscoveryFunc(
	driverConfig *config.Config, discoveryConfig *discoveryConfig.Config,
) func(ctx context.Context, cc *grpc.ClientConn) (endpoints []endpoint.Endpoint, location string, err error) {
	return func(ctx context.Context, cc *grpc.ClientConn) (endpoints []endpoint.Endpoint, location string, err error) {
		ctx, traceID, err := meta.TraceID(ctx)
		if err != nil {
			return endpoints, location, xerrors.WithStackTrace(
				fmt.Errorf("failed to enrich context with meta, traceID %q: %w", traceID, err),
			)
		}

		ctx, err = driverConfig.Meta().DiscoveryContext(ctx)
		if err != nil {
			return endpoints, location, xerrors.WithStackTrace(
				fmt.Errorf("failed to enrich context with meta, traceID %q: %w", traceID, err),
			)
		}

		endpoints, location, err = internalDiscovery.Discover(ctx,
			Ydb_Discovery_V1.NewDiscoveryServiceClient(cc), discoveryConfig,
		)
		if err != nil {
			return endpoints, location, xerrors.WithStackTrace(
				fmt.Errorf("failed to discover database %q (address %q, traceID %q): %w",
					driverConfig.Database(), driverConfig.Endpoint(), traceID, err,
				),
			)
		}

		return endpoints, location, nil
	}
}

func New(ctx context.Context, driverConfig *config.Config, pool *conn.Pool, opts ...discoveryConfig.Option) (
	b *Balancer, finalErr error,
) {
	if ctx.Err() != nil {
		return nil, xerrors.WithStackTrace(ctx.Err())
	}

	configuredEstimator := driverConfig.Balancer()
	if configuredEstimator == nil {
		configuredEstimator = strategy.RandomChoice()
	}

	onDone := gtrace.DriverOnBalancerInit(driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.New"),
		configuredEstimator.String(),
	)
	defer func() {
		onDone(finalErr)
	}()

	plan := strategy.Compile(configuredEstimator)
	b = &Balancer{
		driverConfig: driverConfig,
		balancer:     plan.Estimator(),
		compiledPlan: &plan,
		pool:         pool,
		address:      "ydb:///" + driverConfig.Endpoint(),
		discoveryConfig: discoveryConfig.New(append(opts,
			discoveryConfig.With(driverConfig.Common),
			discoveryConfig.WithEndpoint(driverConfig.Endpoint()),
			discoveryConfig.WithDatabase(driverConfig.Database()),
			discoveryConfig.WithSecure(driverConfig.Secure()),
			discoveryConfig.WithMeta(driverConfig.Meta()),
		)...),
		localDCDetector: detectLocalDC,
		rnd:             xrand.New(xrand.WithLock()),
	}

	b.discover = makeDiscoveryFunc(b.driverConfig, b.discoveryConfig)

	controller, err := plan.Start(ctx, b)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	b.discoveryController = controller

	return b, nil
}

// StartClusterDiscovery initializes dynamic discovery and starts its background refresh.
func (b *Balancer) StartClusterDiscovery(ctx context.Context) (strategy.Controller, error) {
	if err := b.clusterDiscovery(ctx); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if interval := b.discoveryConfig.Interval(); interval > 0 {
		return repeater.New(xcontext.ValueOnly(ctx),
			interval, b.clusterDiscoveryAttemptWithDial,
			repeater.WithName("discovery"),
			repeater.WithTrace(b.driverConfig.Trace()),
		), nil
	}

	return nil, nil //nolint:nilnil // Disabled background discovery does not need a controller.
}

// UseConfiguredEndpoint initializes a static balancer from the configured endpoint.
func (b *Balancer) UseConfiguredEndpoint(ctx context.Context) (strategy.Controller, error) {
	b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{
		endpoint.New(b.driverConfig.Endpoint()),
	}, strategy.ResolvedLocation{})

	return nil, nil //nolint:nilnil // A static endpoint source does not need a controller.
}

func (b *Balancer) Invoke(
	ctx context.Context,
	method string,
	args any,
	reply any,
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
	var stream grpc.ClientStream
	if err := b.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
		inner, innerErr := cc.NewStream(ctx, desc, method, opts...)
		if innerErr != nil {
			return innerErr
		}
		stream = &streamWrapper{
			ClientStream: inner,
			onErr: func(err error) {
				if IsBadConn(ctx, err, b.driverConfig.ExcludeGRPCCodesForPessimization()...) {
					b.ban(ctx, cc, err)
				}
			},
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return stream, nil
}

func (b *Balancer) wrapCall(ctx context.Context, f func(ctx context.Context, cc conn.Conn) error) (err error) {
	cc, err := b.nextConn(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	defer func() {
		if err != nil && cc.State() != state.Banned &&
			IsBadConn(ctx, err, b.driverConfig.ExcludeGRPCCodesForPessimization()...) {
			b.ban(ctx, cc, err)
		}
	}()

	if err = f(conn.WithBanCallback(ctx, func(cause error) {
		b.ban(ctx, cc, cause)
	}), cc); err != nil {
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

func (b *Balancer) forceDiscovery() {
	b.closeMu.Lock()
	controller := b.discoveryController
	b.closeMu.Unlock()

	if controller != nil {
		controller.Force()
	}
}

func (b *Balancer) nextConn(ctx context.Context) (c conn.Conn, err error) {
	onDone := gtrace.DriverOnBalancerChooseEndpoint(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).nextConn"),
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

	if state == nil {
		return nil, xerrors.WithStackTrace(errBalancerClosed)
	}

	if nodeID, ok := endpoint.ContextNodeID(ctx); ok {
		if cc := state.preferConnection(ctx); cc != nil {
			return cc, nil
		}
		if cc := b.ensurePinnedConn(ctx, nodeID); cc != nil {
			return cc, nil
		}
		if !endpoint.ContextFallback(ctx) {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("%w: pinned node %d is not available", ErrNoEndpoints, nodeID),
			)
		}
	}
	if len(state.estimates) == 0 {
		return nil, xerrors.WithStackTrace(ErrNoEndpoints)
	}

	preferredCount := state.PreferredCount()
	defer func() {
		if failedCount*2 <= preferredCount {
			return
		}

		b.forceDiscovery()
	}()

	c, failedCount = b.nextEstimatedConn(ctx, state)
	if c != nil {
		return c, nil
	}

	return nil, xerrors.WithStackTrace(
		fmt.Errorf("%w: cannot get connection from Balancer after %d attempts", ErrNoEndpoints, failedCount),
	)
}

func (b *Balancer) nextEstimatedConn(ctx context.Context, state *connectionsState) (conn.Conn, int) {
	failedCount := 0
	for range len(state.estimates) + 1 {
		key, selected, allowBanned, ok := state.NextEndpoint(ctx)
		if !ok {
			break
		}
		if selected == nil {
			selected = b.ensureEndpointConn(ctx, key)
		}
		if selected != nil && isOkConnection(selected, allowBanned) {
			return selected, failedCount
		}
		failedCount++
		state.Pessimize(key)
		if current := b.connections(); current != state {
			if current == nil {
				break
			}
			current.Pessimize(key)
			state = current
		}
	}

	return nil, failedCount
}

func (b *Balancer) ban(ctx context.Context, connection conn.Conn, cause error) {
	b.pool.Ban(ctx, connection, cause)
	if current := b.connections(); current != nil {
		current.Pessimize(connection.Endpoint().Key())
	}
}

// ensurePinnedConn adds a discovered endpoint omitted from the active set.
// This is the soft-limit escape hatch required by session and topic affinity.
func (b *Balancer) ensurePinnedConn(ctx context.Context, nodeID uint32) conn.Conn {
	var (
		key   endpoint.Key
		found bool
	)
	if current := b.connections(); current != nil {
		for _, candidate := range current.endpointByKey {
			if candidate.NodeID() == nodeID {
				key = candidate.Key()
				found = true

				break
			}
		}
	}
	if !found {
		return nil
	}

	return b.ensureEndpointConn(ctx, key)
}

func (b *Balancer) ensureEndpointConn(ctx context.Context, key endpoint.Key) conn.Conn {
	selected, rejected := b.tryEnsureEndpointConn(key)
	if rejected != nil {
		b.pool.Put(ctx, rejected)
	}

	return selected
}

func (b *Balancer) tryEnsureEndpointConn(key endpoint.Key) (selected, rejected conn.Conn) {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		return nil, nil
	}

	current := b.connectionsState.Load()
	if current != nil {
		if cc := current.Connection(key); cc != nil {
			if !isOkConnection(cc, false) {
				return nil, nil
			}

			return cc, nil
		}
	}

	var target endpoint.Endpoint
	if current != nil {
		target = current.Endpoint(key)
	}
	if target == nil {
		return nil, nil
	}

	cc := b.pool.Get(target)
	if cc == nil {
		return nil, nil
	}
	if !isOkConnection(cc, false) {
		return nil, cc
	}

	active := append([]conn.Conn{cc}, current.All()...)
	activeKeys := current.ActiveKeys()
	activeKeys[key] = struct{}{}
	b.connectionsState.Store(newConnectionsStateWithEstimates(
		active, current.Endpoints(), current.Estimations(), activeKeys, current.quarantine, current.rand,
	))

	return cc, nil
}
