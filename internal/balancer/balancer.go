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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
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

var ErrNoEndpoints = xerrors.Wrap(xerrors.Retryable(fmt.Errorf("no endpoints"), xerrors.WithBackoff(backoff.TypeSlow)))

var (
	errBalancerClosed            = xerrors.Wrap(fmt.Errorf("internal ydb sdk balancer closed"))
	errPeriodicDiscoveryDisabled = xerrors.Wrap(fmt.Errorf(
		"periodic discovery must be enabled for non-single-connection balancer",
	))
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
	driverConfig      *config.Config
	policy            policy.Policy
	discoveryConfig   *discoveryConfig.Config
	pool              *conn.Pool
	discoveryRepeater repeater.Repeater

	address string
	cc      atomic.Pointer[grpc.ClientConn]

	discover        func(context.Context, *grpc.ClientConn) (endpoints []endpoint.Endpoint, location string, err error)
	localDCDetector func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error)

	connectionsState atomic.Pointer[connectionsState]
	random           xrand.Rand
	lastDiscovered   []endpoint.Endpoint

	closeMu sync.Mutex
	closed  bool
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

	if b.policy.DetectsNearestDC() {
		location, err = b.localDCDetector(ctx, endpoints)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
	}
	b.applyDiscoveredEndpoints(ctx, endpoints, location)

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
	selfLocation string,
) {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		b.releaseStateConns(ctx, b.connectionsState.Swap(nil))

		return
	}

	var (
		onDone = gtrace.DriverOnBalancerUpdate(
			b.driverConfig.Trace(), &ctx,
			stack.FunctionID(
				"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).applyDiscoveredEndpoints"),
			b.policy.DetectsNearestDC(),
			b.driverConfig.Database(),
		)
		state              = b.connectionsState.Load()
		active             []conn.Conn
		quarantine         []conn.Conn
		selectedEndpoints  []endpoint.Endpoint
		selectedPriorities []policy.EndpointPriority
	)
	if state != nil {
		active = state.All()
		quarantine = state.quarantine
	}

	defer func() {
		_, added, dropped := xslices.Diff(xslices.Transform(active, func(cc conn.Conn) endpoint.Endpoint {
			return cc.Endpoint()
		}), selectedEndpoints, endpoint.Compare)

		onDone(
			xslices.Transform(endpoints, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
			xslices.Transform(added, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
			xslices.Transform(dropped, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
			selfLocation,
		)
	}()

	info := policy.Info{SelfLocation: selfLocation}
	priorities := b.policy.Prioritize(info, endpoints)
	selectedEndpoints, selectedPriorities = selectActiveEndpoints(
		active, quarantine, endpoints, priorities, b.policy.MaxConnections(), b.random,
	)
	quarantine, connections := nextState(ctx, b.pool, quarantine, active, selectedEndpoints)

	if b.policy.MaxConnections() > 0 {
		b.lastDiscovered = append(b.lastDiscovered[:0], endpoints...)
	}
	b.connectionsState.Store(newConnectionsStateWithPriorities(
		connections, selectedPriorities, quarantine, nil,
	))
}

func (b *Balancer) Close(ctx context.Context) (err error) {
	b.closeMu.Lock()
	if b.closed {
		b.closeMu.Unlock()

		return xerrors.WithStackTrace(errBalancerClosed)
	}

	b.closed = true
	b.lastDiscovered = nil

	oldState := b.connectionsState.Swap(nil)

	discoveryRepeater := b.discoveryRepeater
	b.discoveryRepeater = nil

	discoveryCC := b.cc.Swap(nil)

	b.closeMu.Unlock()

	onDone := gtrace.DriverOnBalancerClose(
		b.driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).Close"),
	)
	defer func() {
		onDone(err)
	}()

	if discoveryRepeater != nil {
		discoveryRepeater.Stop()
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

	policy := driverConfig.Balancer()

	onDone := gtrace.DriverOnBalancerInit(driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.New"),
		policy.String(),
	)
	defer func() {
		onDone(finalErr)
	}()

	b = &Balancer{
		driverConfig: driverConfig,
		policy:       policy,
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
	}
	if policy.MaxConnections() > 0 {
		b.random = xrand.New(xrand.WithLock())
	}

	b.discover = makeDiscoveryFunc(b.driverConfig, b.discoveryConfig)
	if !policy.SingleConnection() && b.discoveryConfig.Interval() <= 0 {
		return nil, xerrors.WithStackTrace(errPeriodicDiscoveryDisabled)
	}

	if policy.SingleConnection() {
		b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{
			endpoint.New(b.driverConfig.Endpoint()),
		}, "")
	} else {
		if err := b.clusterDiscovery(ctx); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		b.discoveryRepeater = repeater.New(xcontext.ValueOnly(ctx),
			b.discoveryConfig.Interval(), b.clusterDiscoveryAttemptWithDial,
			repeater.WithName("discovery"),
			repeater.WithTrace(b.driverConfig.Trace()),
		)
	}

	return b, nil
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
	discoveryRepeater := b.currentDiscoveryRepeater()

	if discoveryRepeater != nil {
		discoveryRepeater.Force()
	}
}

func (b *Balancer) currentDiscoveryRepeater() repeater.Repeater {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	return b.discoveryRepeater
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

	state := b.connections()

	if state == nil {
		return nil, xerrors.WithStackTrace(errBalancerClosed)
	}

	if nodeID, ok := endpoint.ContextNodeID(ctx); ok {
		var handled bool
		c, handled, err = b.pinnedConnection(ctx, state, nodeID)
		if handled {
			return c, err
		}
	}
	if state.elector.CandidateCount() == 0 {
		return nil, xerrors.WithStackTrace(ErrNoEndpoints)
	}

	c, failedCount := b.nextAvailableConn(ctx, state)
	if c != nil {
		return c, nil
	}
	if err = ctx.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return nil, xerrors.WithStackTrace(
		fmt.Errorf("%w: cannot get connection from Balancer after %d attempts", ErrNoEndpoints, failedCount),
	)
}

func (b *Balancer) pinnedConnection(
	ctx context.Context,
	current *connectionsState,
	nodeID uint32,
) (connection conn.Conn, handled bool, err error) {
	if connection := current.preferConnection(ctx); connection != nil {
		return connection, true, nil
	}
	if b.policy.MaxConnections() > 0 {
		connection, rejected := b.tryAddPinnedConnection(nodeID)
		if rejected != nil {
			b.pool.Put(ctx, rejected)
		}
		if connection != nil {
			return connection, true, nil
		}
	}
	if !endpoint.ContextFallback(ctx) {
		return nil, true, xerrors.WithStackTrace(
			fmt.Errorf("%w: pinned node %d is not available", ErrNoEndpoints, nodeID),
		)
	}

	return nil, false, nil
}

func (b *Balancer) nextAvailableConn(ctx context.Context, state *connectionsState) (conn.Conn, int) {
	var failedCount int
	attemptsLeft := state.elector.CandidateCount()
	for attemptsLeft > 0 {
		attemptsLeft--
		if ctx.Err() != nil {
			break
		}
		selected, allowBanned, ok := state.elector.Next()
		if !ok {
			break
		}
		if isConnectionStateUsable(selected.State(), allowBanned) {
			return selected, failedCount
		}
		failedCount++
		b.refreshElection(state.elector)
		if current := b.connections(); current != state {
			if current == nil {
				break
			}
			b.refreshElection(current.elector)
			state = current
			attemptsLeft = max(attemptsLeft, state.elector.CandidateCount())
		}
	}

	return nil, failedCount
}

func (b *Balancer) ban(ctx context.Context, connection conn.Conn, cause error) {
	if b.policy.SingleConnection() {
		return
	}

	alreadyBanned := connection.State() == state.Banned
	b.pool.Ban(ctx, connection, cause)
	forceDiscovery := b.policy.MaxConnections() > 0 && !alreadyBanned
	if current := b.connections(); current != nil {
		forceDiscovery = current.elector.Refresh() || forceDiscovery
	}
	if forceDiscovery {
		b.forceDiscovery()
	}
}

func (b *Balancer) tryAddPinnedConnection(nodeID uint32) (connection, rejected conn.Conn) {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		return nil, nil
	}

	current := b.connectionsState.Load()
	if current == nil {
		return nil, nil
	}
	if existing := current.connByNodeID[nodeID]; existing != nil {
		if isConnectionStateUsable(existing.State(), false) {
			return existing, nil
		}

		return nil, nil
	}

	var discovered endpoint.Endpoint
	for _, candidate := range b.lastDiscovered {
		if candidate.NodeID() == nodeID {
			discovered = candidate

			break
		}
	}
	if discovered == nil {
		return nil, nil
	}

	connection = b.pool.Get(discovered)
	if connection == nil {
		return nil, nil
	}
	if !isConnectionStateUsable(connection.State(), false) {
		return nil, connection
	}

	connections := append(current.All(), connection)
	priorities := append([]policy.EndpointPriority(nil), current.elector.priorities...)
	priorities = append(priorities, policy.EndpointPriority{
		Key:      discovered.Key(),
		Excluded: true,
	})
	b.connectionsState.Store(newConnectionsStateWithPriorities(
		connections, priorities, current.quarantine, current.elector.rand,
	))

	return connection, nil
}

func (b *Balancer) refreshElection(elector *endpointElector) {
	if elector.Refresh() {
		b.forceDiscovery()
	}
}
