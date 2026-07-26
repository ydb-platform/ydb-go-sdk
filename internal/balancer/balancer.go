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
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
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
	driverConfig      *config.Config
	balancerConfig    balancerConfig.Config
	discoveryConfig   *discoveryConfig.Config
	pool              *conn.Pool
	discoveryRepeater repeater.Repeater
	rnd               xrand.Rand

	address string
	cc      atomic.Pointer[grpc.ClientConn]

	discover        func(context.Context, *grpc.ClientConn) (endpoints []endpoint.Endpoint, location string, err error)
	localDCDetector func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error)

	connectionsState atomic.Pointer[connectionsState]

	// lastDiscovered is the full endpoint list from the latest discovery (before
	// MaxConnections sticky selection). Used to replace banned connections and
	// to dial pin/overflow targets outside the active set.
	lastDiscovered []endpoint.Endpoint
	selfLocation   string

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

	if b.balancerConfig.DetectNearestDC {
		location, err := b.localDCDetector(ctx, endpoints)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		b.applyDiscoveredEndpoints(ctx, endpoints, location)
	} else {
		b.applyDiscoveredEndpoints(ctx, endpoints, location)
	}

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

func (b *Balancer) applyDiscoveredEndpoints(ctx context.Context, endpoints []endpoint.Endpoint, localDC string) {
	var (
		onDone = gtrace.DriverOnBalancerUpdate(
			b.driverConfig.Trace(), &ctx,
			stack.FunctionID(
				"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).applyDiscoveredEndpoints"),
			b.balancerConfig.DetectNearestDC,
			b.driverConfig.Database(),
		)
		active, quarantine, connections []conn.Conn
		selected                        []endpoint.Endpoint
	)
	defer func() {
		_, added, dropped := xslices.Diff(xslices.Transform(active, func(cc conn.Conn) endpoint.Endpoint {
			return cc.Endpoint()
		}), selected, endpoint.Compare)

		onDone(
			xslices.Transform(endpoints, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
			xslices.Transform(added, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
			xslices.Transform(dropped, func(e endpoint.Endpoint) trace.EndpointInfo { return e }),
			localDC,
		)
	}()

	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		b.releaseStateConns(ctx, b.connectionsState.Swap(nil))

		return
	}

	state := b.connectionsState.Load()
	if state != nil {
		active = state.All()
		quarantine = state.quarantine
	}

	selected = selectEndpoints(
		active,
		endpoints,
		b.balancerConfig.MaxConnections,
		b.balancerConfig.Filter,
		balancerConfig.Info{SelfLocation: localDC},
		b.balancerConfig.AllowFallback,
		b.rnd,
	)
	quarantine, connections = nextState(ctx, b.pool, quarantine, active, selected)

	b.lastDiscovered = append([]endpoint.Endpoint(nil), endpoints...)
	b.selfLocation = localDC
	b.connectionsState.Store(
		newConnectionsState(connections,
			b.balancerConfig.Filter,
			balancerConfig.Info{SelfLocation: localDC},
			b.balancerConfig.AllowFallback,
			quarantine,
		),
	)
}

// handleBan pessimizes the connection and, when MaxConnections is set, evicts it
// from the active set so a live discovered endpoint can take its slot.
func (b *Balancer) handleBan(ctx context.Context, cc conn.Conn, cause error) {
	b.pool.Ban(ctx, cc, cause)
	b.replaceBannedConn(ctx, cc)
}

func (b *Balancer) replaceBannedConn(ctx context.Context, banned conn.Conn) {
	if b.balancerConfig.MaxConnections <= 0 || banned == nil {
		return
	}

	released, rep := b.swapBannedConn(ctx, banned)
	if rep != nil {
		rep.Force()

		return
	}
	if released == nil {
		return
	}

	// Release outside the lock and asynchronously: Ban may run from stream
	// RecvMsg, and a synchronous pool.Put/Close can deadlock waiting for that Recv.
	go b.pool.Put(xcontext.ValueOnly(ctx), released)
}

// swapBannedConn evicts banned from the active set when another discovered
// endpoint can take its MaxConnections slot. Returns the connection to release
// and an optional discovery repeater to force when no replacement exists.
func (b *Balancer) swapBannedConn(ctx context.Context, banned conn.Conn) (
	released conn.Conn,
	force repeater.Repeater,
) {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		return nil, nil
	}

	state := b.connectionsState.Load()
	if state == nil {
		return nil, nil
	}

	active := state.All()
	idx := connectionIndex(active, banned)
	if idx < 0 {
		return nil, nil
	}

	withoutBanned := append(active[:idx:idx], active[idx+1:]...)
	replacement := replacementEndpoint(
		b.lastDiscovered,
		withoutBanned,
		banned.Endpoint().Key(),
		b.balancerConfig.Filter,
		balancerConfig.Info{SelfLocation: b.selfLocation},
		b.balancerConfig.AllowFallback,
	)
	if replacement == nil {
		// No other discovered endpoint can take the slot. Keep the banned
		// connection in the active set (usable as last resort / for retry).
		return nil, b.discoveryRepeater
	}

	newActive := withoutBanned
	if cc := b.pool.Get(replacement); cc != nil {
		cc.Unban(ctx)
		newActive = append(newActive, cc)
	}

	b.connectionsState.Store(
		newConnectionsState(newActive,
			b.balancerConfig.Filter,
			balancerConfig.Info{SelfLocation: b.selfLocation},
			b.balancerConfig.AllowFallback,
			state.quarantine,
		),
	)

	return banned, nil
}

func (b *Balancer) Close(ctx context.Context) (err error) {
	b.closeMu.Lock()
	if b.closed {
		b.closeMu.Unlock()

		return xerrors.WithStackTrace(errBalancerClosed)
	}

	b.closed = true

	oldState := b.connectionsState.Swap(nil)

	rep := b.discoveryRepeater
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

	if rep != nil {
		rep.Stop()
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

	rnd, err := xrand.NewCryptoSeeded(xrand.WithLock())
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("initialize balancer random generator: %w", err))
	}

	onDone := gtrace.DriverOnBalancerInit(driverConfig.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.New"),
		driverConfig.Balancer().String(),
	)
	defer func() {
		onDone(finalErr)
	}()

	b = &Balancer{
		driverConfig: driverConfig,
		pool:         pool,
		rnd:          rnd,
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

	b.discover = makeDiscoveryFunc(b.driverConfig, b.discoveryConfig)

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
		if err := b.clusterDiscovery(ctx); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		// run background discovering
		if d := b.discoveryConfig.Interval(); d > 0 {
			b.discoveryRepeater = repeater.New(xcontext.ValueOnly(ctx),
				d, b.clusterDiscoveryAttemptWithDial,
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
					b.handleBan(ctx, cc, err)
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
			b.handleBan(ctx, cc, err)
		}
	}()

	if err = f(conn.WithBanCallback(ctx, func(cause error) {
		b.handleBan(ctx, cc, cause)
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

func (b *Balancer) forceDiscoveryIfNeeded(failedCount *int, preferredCount int) {
	if *failedCount*2 <= preferredCount {
		return
	}

	b.closeMu.Lock()
	rep := b.discoveryRepeater
	b.closeMu.Unlock()

	if rep != nil {
		rep.Force()
	}
}

// nextConn returns a connection for the RPC.
//
// When MaxConnections is set and the caller pins a node outside the active set,
// the balancer may soft-exceed the limit by adding that endpoint to the active
// set (needed for session/topic affinity and long-lived streams).
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

	if len(state.all) == 0 {
		return nil, xerrors.WithStackTrace(ErrNoEndpoints)
	}

	defer b.forceDiscoveryIfNeeded(&failedCount, state.PreferredCount())

	// Soft-limit overflow: pin to a node outside the active MaxConnections set.
	if nodeID, ok := endpoint.ContextNodeID(ctx); ok {
		if cc := state.preferConnection(ctx); cc != nil {
			return cc, nil
		}
		if cc := b.ensurePinnedConn(ctx, nodeID); cc != nil {
			return cc, nil
		}
		if !endpoint.ContextFallback(ctx) {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("%w: pinned node %d is outside balancer active set", ErrNoEndpoints, nodeID),
			)
		}
		// Pin already checked above; skip a redundant preferConnection lookup.
		c, failedCount = state.GetConnectionUnpinned(ctx)
	} else {
		c, failedCount = state.GetConnection(ctx)
	}
	if c != nil {
		return c, nil
	}

	return nil, xerrors.WithStackTrace(
		fmt.Errorf("%w: cannot get connection from Balancer after %d attempts", ErrNoEndpoints, failedCount),
	)
}

// ensurePinnedConn adds a discovered endpoint for nodeID to the active set when
// it is missing (soft-exceeding MaxConnections). Discovery sticky select will
// shrink the set back on the next update when possible.
func (b *Balancer) ensurePinnedConn(ctx context.Context, nodeID uint32) conn.Conn {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		return nil
	}

	connState := b.connectionsState.Load()
	if connState != nil {
		if cc := connState.connByNodeID[nodeID]; cc != nil && isOkConnection(cc, false) {
			return cc
		}
	}

	e := endpointByNodeID(b.lastDiscovered, nodeID)
	if e == nil {
		return nil
	}

	cc := b.pool.Get(e)
	if cc == nil {
		return nil
	}
	cc.Unban(ctx)

	active := []conn.Conn{cc}
	quarantine := []conn.Conn(nil)
	if connState != nil {
		active = append(connState.All(), cc)
		quarantine = connState.quarantine
	}

	b.connectionsState.Store(
		newConnectionsState(active,
			b.balancerConfig.Filter,
			balancerConfig.Info{SelfLocation: b.selfLocation},
			b.balancerConfig.AllowFallback,
			quarantine,
		),
	)

	return cc
}
