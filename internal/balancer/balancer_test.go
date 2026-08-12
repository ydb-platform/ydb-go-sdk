package balancer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	userBalancers "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var errNodeShutdownHint = errors.New("received node shutdown hint")

// poolRegisteredConn routes gRPC to a mock and state to a pool-registered connection.
type poolRegisteredConn struct {
	grpc.ClientConnInterface

	inner conn.Conn
}

func (c *poolRegisteredConn) Ban(ctx context.Context) {
	c.inner.Ban(ctx)
}

func (c *poolRegisteredConn) Endpoint() endpoint.Endpoint {
	return c.inner.Endpoint()
}

func (c *poolRegisteredConn) State() state.State {
	return c.inner.State()
}

func (c *poolRegisteredConn) Unban(ctx context.Context) {
	c.inner.Unban(ctx)
}

func TestBalancer_discoveryConn(t *testing.T) {
	// testTimeout defines the test timeout and is an example of an actual user-defined timeout.
	//
	// I couldn't find any events for synchronization, assuming that there
	// might be retries with different logic and their own timeouts inside `discoveryConn`.
	// If not now, then in the future. One second excludes false test failures in case
	// the test is run on very slow workers. Moreover, one second is only lost if the test fails,
	// and in that case, losing it is not critical. Upon successful completion of the test,
	// the context is canceled via `cancel()`.
	const testTimeout = 1 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	fakeListener := bufconn.Listen(1024 * 1024)
	defer fakeListener.Close()

	fakeServer := grpc.NewServer()
	defer fakeServer.Stop()

	go func() {
		_ = fakeServer.Serve(fakeListener)
	}()

	var dialAttempt atomic.Uint32

	balancer := &Balancer{
		address: "ydbmock:///mock",
		driverConfig: config.New(
			config.WithEndpoint("mock"),
			config.WithGrpcOptions(
				grpc.WithResolvers(&mockResolverBuilder{}),

				grpc.WithContextDialer(
					// The first dialing is never ended, while the subsequent ones work fine.
					func(ctx context.Context, s string) (net.Conn, error) {
						if dialAttempt.Add(1) == 1 {
							<-ctx.Done() // dial will never complete successfully

							return nil, fmt.Errorf("fake error for endpoint: %s: %w", s, ctx.Err())
						}

						return fakeListener.DialContext(ctx)
					}),

				// If you want to reproduce the issue, uncomment the line:
				// grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy": "pick_first"}`),
			),
		),
	}

	_, err := balancer.discoveryConn(ctx)
	require.NoError(t, err)
}

func TestApplyDiscoveredEndpoints(t *testing.T) {
	ctx := context.Background()

	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	defer func() { _ = pool.RemoveRef(ctx) }()

	b := &Balancer{
		driverConfig: cfg,
		pool:         pool,
	}

	initial := newConnectionsState(nil, nil, policy.Info{}, nil)
	b.connectionsState.Store(initial)

	e1 := endpoint.New("e1.example:2135", endpoint.WithIPV6([]string{"2001:db8::1"}), endpoint.WithID(1))
	e2 := endpoint.New("e2.example:2135", endpoint.WithIPV6([]string{"2001:db8::2"}), endpoint.WithID(2))

	// call with two endpoints
	b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{e1, e2}, "")

	// connectionsState should be updated and reflect the endpoints
	after := b.connections()
	require.NotNil(t, after)
	all := after.All()
	require.Equal(t, 2, len(all))
	require.Equal(t, e1.Address(), all[0].Endpoint().Address())
	require.Equal(t, e1.NodeID(), all[0].Endpoint().NodeID())
	require.Equal(t, e2.Address(), all[1].Endpoint().Address())
	require.Equal(t, e2.NodeID(), all[1].Endpoint().NodeID())

	// partially replace endpoints
	e3 := endpoint.New("e3.example:2135", endpoint.WithIPV6([]string{"2001:db8::3"}), endpoint.WithID(1))
	b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{e2, e3}, "")
	// connectionsState should be updated and reflect the endpoints
	after = b.connections()
	require.NotNil(t, after)
	all = after.All()
	require.Equal(t, 2, len(all))
	require.Equal(t, e2.Address(), all[0].Endpoint().Address())
	require.Equal(t, e2.NodeID(), all[0].Endpoint().NodeID())
	require.Equal(t, e3.Address(), all[1].Endpoint().Address())
	require.Equal(t, e3.NodeID(), all[1].Endpoint().NodeID())
}

func TestApplyDiscoveredEndpointsKeepsFilteredConnectionsUntilDiscoveryDropsThem(t *testing.T) {
	ctx := context.Background()

	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	defer func() { _ = pool.RemoveRef(ctx) }()

	b := &Balancer{
		driverConfig: cfg,
		pool:         pool,
	}

	e1 := endpoint.New("e1.example:2135", endpoint.WithID(1))
	e2 := endpoint.New("e2.example:2135", endpoint.WithID(2))

	b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{e1, e2}, "")
	require.Len(t, b.connections().All(), 2)
	require.Equal(t, []policy.EndpointPriority{
		{Key: e1.Key()},
		{Key: e2.Key()},
	}, b.connections().elector.priorities)

	b.policy = policy.Prefer(
		policy.Policy{}, "NodeID(1)",
		func(_ policy.Info, candidate endpoint.Info) bool {
			return candidate.NodeID() == 1
		},
	)
	b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{e1, e2}, "")
	require.Len(t, b.connections().All(), 2, "selection policy must not change connection ownership")
	require.Equal(t, []policy.EndpointPriority{
		{Key: e1.Key()},
		{Key: e2.Key(), Priority: 1},
	}, b.connections().elector.priorities)
	selected, err := b.nextConn(endpoint.WithNodeID(ctx, 2))
	require.NoError(t, err)
	require.Equal(t, e2.Key(), selected.Endpoint().Key())

	b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{e1}, "")
	require.NotNil(t, connInQuarantine(b, 2), "filtered-out conn must stay in quarantine until released")

	b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{e1}, "")
	require.Nil(t, connInQuarantine(b, 2), "filtered-out conn must be released after quarantine cycle")
}

func TestApplyDiscoveredEndpointsClosedPool(t *testing.T) {
	ctx := context.Background()
	pool := conn.NewPool(ctx, config.New())
	require.NoError(t, pool.RemoveRef(ctx))

	b := &Balancer{
		driverConfig: config.New(),
		pool:         pool,
	}

	require.NotPanics(t, func() {
		b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{
			endpoint.New("node:2135", endpoint.WithID(1)),
		}, "")
	})
}

func TestBalancer_Close(t *testing.T) {
	t.Run("InvokeAfterCloseReturnsBalancerClosed", func(t *testing.T) {
		ctx := context.Background()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		cc := &mock.Conn{
			ClientConnInterface: &grpc.ClientConn{},
			AddrField:           "node:2135",
			NodeIDField:         1,
		}

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		b.connectionsState.Store(newConnectionsState(
			[]conn.Conn{cc},
			nil,
			policy.Info{},
			nil,
		))

		require.NoError(t, b.Close(ctx))

		err := b.Invoke(ctx, "/test.Service/Method", nil, nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, errBalancerClosed))
	})

	t.Run("IsIdempotentUnderConcurrency", func(t *testing.T) {
		ctx := context.Background()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		b.connectionsState.Store(newConnectionsState(nil,
			nil, policy.Info{}, nil,
		))

		var wg sync.WaitGroup
		errs := make(chan error, 8)
		wg.Add(8)

		for range 8 {
			go func() {
				defer wg.Done()
				errs <- b.Close(ctx)
			}()
		}

		wg.Wait()
		close(errs)

		var closedErrs int
		for err := range errs {
			if errors.Is(err, errBalancerClosed) {
				closedErrs++
			} else {
				require.NoError(t, err)
			}
		}

		require.Equal(t, 7, closedErrs)
		require.True(t, b.closed)
	})

	t.Run("ApplyDiscoveredEndpointsDuringCloseDoesNotPanic", func(t *testing.T) {
		ctx := context.Background()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		b.connectionsState.Store(newConnectionsState(nil,
			nil, policy.Info{}, nil,
		))

		closeStarted := make(chan struct{})
		go func() {
			close(closeStarted)
			_ = b.Close(ctx)
		}()

		<-closeStarted
		time.Sleep(10 * time.Millisecond)

		require.NotPanics(t, func() {
			b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{
				endpoint.New("late-discovery:2135", endpoint.WithID(1)),
			}, "")
		})
	})

	t.Run("CloseReleasesCloseMuBeforeStop", func(t *testing.T) {
		ctx := context.Background()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		b.connectionsState.Store(newConnectionsState(nil,
			nil, policy.Info{}, nil,
		))

		stopCalled := make(chan struct{})
		closeMuFreeDuringStop := false
		b.discoveryRepeater = &stubRepeater{
			stopFn: func() {
				if b.closeMu.TryLock() {
					closeMuFreeDuringStop = true
					b.closeMu.Unlock()
				}
				close(stopCalled)
			},
		}

		require.NoError(t, b.Close(ctx))
		require.True(t, closeMuFreeDuringStop, "closeMu must be released before repeater Stop")
		<-stopCalled
	})

	t.Run("CloseClosesDiscoveryConn", func(t *testing.T) {
		ctx := context.Background()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		lis := bufconn.Listen(1024 * 1024)
		t.Cleanup(func() { _ = lis.Close() })

		srv := grpc.NewServer()
		t.Cleanup(srv.Stop)
		go func() { _ = srv.Serve(lis) }()

		cc, err := grpc.NewClient("passthrough:///test",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = cc.Close() })

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		b.connectionsState.Store(newConnectionsState(nil,
			nil, policy.Info{}, nil,
		))
		b.cc.Store(cc)

		require.NoError(t, b.Close(ctx))
		require.Equal(t, connectivity.Shutdown, cc.GetState())
	})

	t.Run("NextConnNoEndpoints", func(t *testing.T) {
		ctx := context.Background()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		b.connectionsState.Store(newConnectionsState(
			nil,
			nil,
			policy.Info{},
			nil,
		))

		_, err := b.nextConn(ctx)
		require.ErrorIs(t, err, ErrNoEndpoints)
	})

	t.Run("ApplyDiscoveredEndpointsWhenBalancerClosed", func(t *testing.T) {
		ctx := context.Background()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		e := endpoint.New("closed-balancer:2135", endpoint.WithID(1))
		c := pool.Get(e)

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
			closed:       true,
		}
		b.connectionsState.Store(newConnectionsState(
			[]conn.Conn{c},
			nil,
			policy.Info{},
			nil,
		))

		b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{e}, "")
		require.Nil(t, b.connectionsState.Load())

		c2 := pool.Get(e)
		require.NotNil(t, c2)
		require.NotSame(t, c, c2)
	})
}

func TestBalancer_CloseRacesWithNextConnRepeater(t *testing.T) {
	ctx := t.Context()
	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	defer func() { _ = pool.RemoveRef(ctx) }()

	stateEntered := make(chan struct{})
	releaseState := make(chan struct{})
	baseConn := &mock.Conn{
		AddrField:   "blocked:2135",
		NodeIDField: 1,
		StateField:  state.Online,
	}
	blockingConn := &blockingStateConn{
		Conn:         baseConn,
		stateEntered: stateEntered,
		releaseState: releaseState,
	}

	b := &Balancer{
		driverConfig:      cfg,
		pool:              pool,
		discoveryRepeater: &stubRepeater{},
	}
	connections := newConnectionsState(
		[]conn.Conn{baseConn},
		nil,
		policy.Info{},
		nil,
	)
	connections.all[0] = blockingConn
	connections.elector.connections[baseConn.Endpoint().Key()] = blockingConn
	connections.elector.snapshot.Load().connections[0] = blockingConn
	connections.connByNodeID[baseConn.Endpoint().NodeID()] = blockingConn
	b.connectionsState.Store(connections)

	nextConnDone := make(chan error, 1)
	go func() {
		_, err := b.nextConn(ctx)
		nextConnDone <- err
	}()

	<-stateEntered

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- b.Close(ctx)
	}()

	close(releaseState)

	require.ErrorIs(t, <-nextConnDone, ErrNoEndpoints)
	require.NoError(t, <-closeDone)
}

func TestBalancer_CloseWhileDiscoveryDialInFlight(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	t.Cleanup(srv.Stop)
	go func() { _ = srv.Serve(lis) }()

	dialStarted := make(chan struct{})
	continueDial := make(chan struct{})
	cfg := config.New(
		config.WithEndpoint("test"),
		config.WithGrpcOptions(
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				close(dialStarted)
				select {
				case <-continueDial:
					return lis.DialContext(ctx)
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		),
	)
	pool := conn.NewPool(ctx, cfg)
	defer func() { _ = pool.RemoveRef(ctx) }()

	b := &Balancer{
		address:      "passthrough:///test",
		driverConfig: cfg,
		pool:         pool,
	}
	b.connectionsState.Store(newConnectionsState(
		nil,
		nil,
		policy.Info{},
		nil,
	))

	type discoveryConnResult struct {
		cc  *grpc.ClientConn
		err error
	}
	result := make(chan discoveryConnResult, 1)
	go func() {
		cc, err := b.discoveryConn(ctx)
		result <- discoveryConnResult{cc: cc, err: err}
	}()

	<-dialStarted
	closeErr := b.Close(ctx)
	close(continueDial)
	require.NoError(t, closeErr)

	res := <-result
	require.ErrorIs(t, res.err, errBalancerClosed)
	require.Nil(t, res.cc)

	if leaked := b.cc.Load(); leaked != nil {
		t.Fatalf("an in-flight discovery dial published a connection in state %s after Balancer.Close", leaked.GetState())
	}
}

type blockingStateConn struct {
	conn.Conn

	once         sync.Once
	stateEntered chan struct{}
	releaseState <-chan struct{}
}

func (c *blockingStateConn) State() state.State {
	c.once.Do(func() {
		close(c.stateEntered)
	})
	<-c.releaseState

	return state.Destroyed
}

type stubRepeater struct {
	stopFn  func()
	forceFn func()
}

func (s *stubRepeater) Stop() {
	if s.stopFn != nil {
		s.stopFn()
	}
}

func (s *stubRepeater) Force() {
	if s.forceFn != nil {
		s.forceFn()
	}
}

// Mock resolver
//

type mockResolverBuilder struct{}

func (r *mockResolverBuilder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (
	resolver.Resolver, error,
) {
	state := resolver.State{Addresses: []resolver.Address{
		{Addr: "mockaddress1"},
		{Addr: "mockaddress2"},
	}}
	_ = cc.UpdateState(state)

	return &mockResolver{}, nil
}

func (r *mockResolverBuilder) Scheme() string { return "ydbmock" }

type mockResolver struct{}

func (r *mockResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (r *mockResolver) Close()                                {}

func TestNew(t *testing.T) {
	t.Run("context already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := New(ctx, config.New(), nil)
		require.ErrorIs(t, err, context.Canceled)
		assert.Regexp(t, "^context canceled at", err.Error())
	})
	t.Run("default policy", func(t *testing.T) {
		ctx := t.Context()
		srv := startDynamicDiscoveryServer(t, []uint32{1})
		cfg := config.New(
			config.WithEndpoint(srv.endpoint()),
			config.WithDatabase("/local"),
			config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
		pool := conn.NewPool(ctx, cfg)
		defer func() { require.NoError(t, pool.RemoveRef(ctx)) }()

		b, err := New(ctx, cfg, pool)
		require.NoError(t, err)
		require.Equal(t, "Priority", b.policy.String())
		require.NoError(t, b.Close(ctx))
	})
	t.Run("single connection policy skips discovery", func(t *testing.T) {
		ctx := t.Context()
		cfg := config.New(
			config.WithEndpoint("bootstrap:2135"),
			config.WithBalancer(policy.SingleConn()),
		)
		pool := conn.NewPool(ctx, cfg)
		defer func() { require.NoError(t, pool.RemoveRef(ctx)) }()

		b, err := New(ctx, cfg, pool)
		require.NoError(t, err)
		require.Len(t, b.connections().All(), 1)
		require.Equal(t, "bootstrap:2135", b.connections().All()[0].Endpoint().Address())
		require.Nil(t, b.discoveryRepeater)
		require.NoError(t, b.Close(ctx))
	})
	t.Run("non-single policy requires periodic discovery", func(t *testing.T) {
		ctx := t.Context()
		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { require.NoError(t, pool.RemoveRef(ctx)) }()

		b, err := New(ctx, cfg, pool, discoveryConfig.WithInterval(-time.Nanosecond))

		require.ErrorIs(t, err, errPeriodicDiscoveryDisabled)
		require.Nil(t, b)
	})
}

func TestBalancerForceDiscovery(t *testing.T) {
	forceCalled := false
	closeMuFreeDuringForce := false
	b := &Balancer{}
	b.discoveryRepeater = &stubRepeater{
		forceFn: func() {
			forceCalled = true
			if b.closeMu.TryLock() {
				closeMuFreeDuringForce = true
				b.closeMu.Unlock()
			}
		},
	}

	b.forceDiscovery()

	require.True(t, forceCalled)
	require.True(t, closeMuFreeDuringForce, "closeMu must be released before repeater Force")
}

func TestBalancerForcesDiscoveryWhenMostConnectionsAreBanned(t *testing.T) {
	ctx := t.Context()
	preferred := &mock.Conn{
		AddrField: "preferred", NodeIDField: 1, LocationField: "preferred", StateField: state.Online,
	}
	firstFallback := &mock.Conn{
		AddrField: "first-fallback", NodeIDField: 2, LocationField: "fallback", StateField: state.Online,
	}
	secondFallback := &mock.Conn{
		AddrField: "second-fallback", NodeIDField: 3, LocationField: "fallback", StateField: state.Online,
	}
	p := policy.Prefer(
		policy.Policy{}, "preferred",
		func(_ policy.Info, candidate endpoint.Info) bool {
			return candidate.Location() == "preferred"
		},
	)
	forceCalls := 0
	pool := conn.NewPool(ctx, config.New())
	defer func() { require.NoError(t, pool.RemoveRef(ctx)) }()
	balancer := &Balancer{
		driverConfig: config.New(), pool: pool,
		discoveryRepeater: &stubRepeater{forceFn: func() {
			forceCalls++
		}},
	}
	balancer.connectionsState.Store(newConnectionsStateWithPolicy(
		[]conn.Conn{preferred, firstFallback, secondFallback}, p, policy.Info{}, nil,
	))

	balancer.ban(ctx, preferred, errors.New("preferred unavailable"))
	require.Zero(t, forceCalls, "one banned preferred endpoint is less than half of all connections")

	selected, err := balancer.nextConn(ctx)
	require.NoError(t, err)
	require.Contains(t, []conn.Conn{firstFallback, secondFallback}, selected)

	balancer.ban(ctx, firstFallback, errors.New("first fallback unavailable"))
	require.Equal(t, 1, forceCalls, "two of three banned connections must force discovery")
	balancer.ban(ctx, firstFallback, errors.New("duplicate ban"))
	require.Equal(t, 1, forceCalls, "remaining above the threshold must not force discovery repeatedly")

	selected, err = balancer.nextConn(ctx)
	require.NoError(t, err)
	require.Same(t, secondFallback, selected)
}

func TestSingleConnectionPolicyDoesNotBanItsOnlyConnection(t *testing.T) {
	connection := &mock.Conn{AddrField: "entrypoint", StateField: state.Online}
	balancer := &Balancer{
		driverConfig: config.New(),
		policy:       policy.SingleConn(),
	}
	balancer.connectionsState.Store(newConnectionsStateWithPolicy(
		[]conn.Conn{connection}, balancer.policy, policy.Info{}, nil,
	))

	balancer.ban(t.Context(), connection, errors.New("temporary failure"))
	selected, err := balancer.nextConn(t.Context())

	require.NoError(t, err)
	require.Same(t, connection, selected)
	require.Equal(t, state.Online, connection.State())
}

// TestPessimizationOnOverloaded verifies that calling Invoke with a context tagged via
// conn.BanOnOperationError causes the balancer to ban the connection that returns OVERLOADED,
// and that when all connections are pessimized the balancer still returns a connection
// (falling back to banned connections).
func TestPessimizationOnOverloaded(t *testing.T) {
	ctx := context.Background()

	overloadedErr := xerrors.WithStackTrace(xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED),
	))

	t.Run("HintBanRemainsAfterSuccessfulRPCWhenConnInPool", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			ctrl := gomock.NewController(t)
			grpcCC := mock.NewMockClientConnInterface(ctrl)
			gomock.InOrder(
				grpcCC.EXPECT().Invoke(
					gomock.Any(),
					gomock.Any(),
					gomock.Any(),
					gomock.Any(),
				).DoAndReturn(func(ctx context.Context, _ string, _, _ any, _ ...grpc.CallOption) error {
					conn.Ban(ctx, errNodeShutdownHint)

					return nil
				}),
				grpcCC.EXPECT().Invoke(
					gomock.Any(),
					gomock.Any(),
					gomock.Any(),
					gomock.Any(),
				).Return(nil),
			)

			cfg := config.New()
			pool := conn.NewPool(ctx, cfg)
			defer func() { _ = pool.RemoveRef(ctx) }()

			e1 := endpoint.New("node1:2135", endpoint.WithID(1))
			poolConn := pool.Get(e1)
			conn.SetState(ctx, poolConn, state.Online)

			cc1 := &poolRegisteredConn{
				inner:               poolConn,
				ClientConnInterface: grpcCC,
			}

			b := &Balancer{
				driverConfig: cfg,
				pool:         pool,
			}
			s := newConnectionsState([]conn.Conn{cc1}, nil, policy.Info{}, nil)
			b.connectionsState.Store(s)

			nodeCtx := endpoint.WithNodeID(ctx, e1.NodeID())

			err := b.Invoke(nodeCtx, "/test.Service/Method", nil, nil)
			require.NoError(t, err)
			assert.Equal(t, state.Banned, cc1.State())

			err = b.Invoke(nodeCtx, "/test.Service/Method", nil, nil)
			require.NoError(t, err)
			assert.Equal(t, state.Banned, cc1.State(),
				"hint-based ban must remain after a successful RPC when the connection is in the pool",
			)
		})
	})

	t.Run("BanCallbackPessimizesConnection", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := mock.NewMockClientConnInterface(ctrl)
		cc.EXPECT().Invoke(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, _ string, _, _ any, _ ...grpc.CallOption) error {
			conn.Ban(ctx, errors.New("node shutdown hint"))

			return nil
		})

		cc1 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node1:2135",
			NodeIDField:         1,
			StateField:          state.Online,
		}
		cc2 := &mock.Conn{
			AddrField: "node2:2135", NodeIDField: 2, StateField: state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, policy.Info{}, nil)
		b.connectionsState.Store(s)

		err := b.Invoke(endpoint.WithNodeID(ctx, cc1.NodeIDField), "/test.Service/Method", nil, nil)
		require.NoError(t, err)
		require.Equal(t, state.Banned, cc1.State())

		for range 10 {
			c, nextErr := b.nextConn(ctx)
			require.NoError(t, nextErr)
			require.Equal(t, cc2.AddrField, c.Endpoint().Address())
		}
	})

	t.Run("HappyPath", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := mock.NewMockClientConnInterface(ctrl)
		cc.EXPECT().Invoke(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(context.Context, string, any, any, ...grpc.CallOption) error {
			return nil
		})

		cc1 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node1:2135", NodeIDField: 1, StateField: state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		s := newConnectionsState([]conn.Conn{cc1}, nil, policy.Info{}, nil)
		b.connectionsState.Store(s)

		err := b.Invoke(ctx, "/test.Service/Method", nil, nil)
		require.NoError(t, err)
		require.NotEqual(t, state.Banned, cc1.State())
	})

	t.Run("PessimizedConnectionExcludedFromBalancing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := mock.NewMockClientConnInterface(ctrl)
		cc.EXPECT().Invoke(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(context.Context, string, any, any, ...grpc.CallOption) error {
			return overloadedErr
		})

		cc1 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node1:2135",
			NodeIDField:         1,
			StateField:          state.Online,
		}
		cc2 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node2:2135",
			NodeIDField:         2,
			StateField:          state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, policy.Info{}, nil)
		b.connectionsState.Store(s)

		// Call Invoke targeting cc1 with OVERLOADED tagged in context — wrapCall must ban cc1.
		invokeCtx := BanOnOperationError(
			endpoint.WithNodeID(ctx, cc1.NodeIDField),
			Ydb.StatusIds_OVERLOADED,
		)
		err := b.Invoke(invokeCtx, "/test.Service/Method", nil, nil)
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED))

		// cc1 must be Banned now.
		require.Equal(t, state.Banned, cc1.State())

		// nextConn must only return cc2 now.
		for range 100 {
			c, nextErr := b.nextConn(ctx)
			require.NoError(t, nextErr)
			require.Equal(t, cc2.AddrField, c.Endpoint().Address())
		}
	})

	t.Run("DoesNotBanConnectionOnOtherOperationErrors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := mock.NewMockClientConnInterface(ctrl)
		cc.EXPECT().Invoke(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(context.Context, string, any, any, ...grpc.CallOption) error {
			return xerrors.WithStackTrace(xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_NOT_FOUND),
			))
		})

		cc1 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node1:2135", NodeIDField: 1, StateField: state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		s := newConnectionsState([]conn.Conn{cc1}, nil, policy.Info{}, nil)
		b.connectionsState.Store(s)

		// Context only bans on OVERLOADED — a NOT_FOUND error must not ban.
		invokeCtx := BanOnOperationError(ctx, Ydb.StatusIds_OVERLOADED)
		err := b.Invoke(invokeCtx, "/test.Service/Method", nil, nil)
		require.Error(t, err)
		require.NotEqual(t, state.Banned, cc1.State())
	})

	t.Run("BansConnectionOnUnavailableForSessionCreate", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := mock.NewMockClientConnInterface(ctrl)
		unavailableErr := xerrors.WithStackTrace(xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE),
		))
		cc.EXPECT().Invoke(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(context.Context, string, any, any, ...grpc.CallOption) error {
			return unavailableErr
		})

		cc1 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node1:2135",
			NodeIDField:         1,
			StateField:          state.Online,
		}
		cc2 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node2:2135",
			NodeIDField:         2,
			StateField:          state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, policy.Info{}, nil)
		b.connectionsState.Store(s)

		invokeCtx := BanOnSessionCreate(endpoint.WithNodeID(ctx, cc1.NodeIDField))
		err := b.Invoke(invokeCtx, "/Ydb.Query.V1.QueryService/CreateSession", nil, nil)
		require.Error(t, err)
		require.Equal(t, state.Banned, cc1.State())

		for range 10 {
			c, nextErr := b.nextConn(ctx)
			require.NoError(t, nextErr)
			require.Equal(t, cc2.AddrField, c.Endpoint().Address())
		}
	})

	t.Run("BansConnectionOnContextDeadlineExceededForSessionCreate", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := mock.NewMockClientConnInterface(ctrl)
		cc.EXPECT().Invoke(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(context.Context, string, any, any, ...grpc.CallOption) error {
			return context.DeadlineExceeded
		})

		cc1 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node1:2135",
			NodeIDField:         1,
			StateField:          state.Online,
		}
		cc2 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node2:2135",
			NodeIDField:         2,
			StateField:          state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, policy.Info{}, nil)
		b.connectionsState.Store(s)

		invokeCtx := BanOnSessionCreate(endpoint.WithNodeID(ctx, cc1.NodeIDField))
		err := b.Invoke(invokeCtx, "/Ydb.Query.V1.QueryService/CreateSession", nil, nil)
		require.Error(t, err)
		require.Equal(t, state.Banned, cc1.State())

		for range 10 {
			c, nextErr := b.nextConn(ctx)
			require.NoError(t, nextErr)
			require.Equal(t, cc2.AddrField, c.Endpoint().Address())
		}
	})

	t.Run("AllConnectionsPessimizedFallback", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := mock.NewMockClientConnInterface(ctrl)
		cc.EXPECT().Invoke(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(context.Context, string, any, any, ...grpc.CallOption) error {
			return overloadedErr
		}).AnyTimes()

		cc1 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node1:2135", NodeIDField: 1, StateField: state.Online,
		}
		cc2 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node2:2135", NodeIDField: 2, StateField: state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, policy.Info{}, nil)
		b.connectionsState.Store(s)

		// Sequentially pessimize cc1 then cc2 via the normal Invoke+BanOnOperationError flow.
		cc1Ctx := BanOnOperationError(endpoint.WithNodeID(ctx, cc1.NodeIDField), Ydb.StatusIds_OVERLOADED)
		err := b.Invoke(cc1Ctx, "/test.Service/Method", nil, nil)
		require.Error(t, err)
		require.Equal(t, state.Banned, cc1.State())

		cc2Ctx := BanOnOperationError(endpoint.WithNodeID(ctx, cc2.NodeIDField), Ydb.StatusIds_OVERLOADED)
		err = b.Invoke(cc2Ctx, "/test.Service/Method", nil, nil)
		require.Error(t, err)
		require.Equal(t, state.Banned, cc2.State())

		// When all connections are banned, the balancer must still return a connection
		// (falling back to the banned connections pool so callers can retry).
		c, err := b.nextConn(ctx)
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, state.Banned, c.State())
	})

	t.Run("StreamSendMsgErrorBansConnection", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := mock.NewMockClientConnInterface(ctrl)
		mockStream := mock.NewMockClientStream(ctrl)
		cc.EXPECT().NewStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
				mockStream.EXPECT().Context().Return(ctx).AnyTimes()
				mockStream.EXPECT().Header().Return(nil, nil).AnyTimes()
				mockStream.EXPECT().Trailer().Return(nil).AnyTimes()
				mockStream.EXPECT().CloseSend().Return(nil).AnyTimes()
				mockStream.EXPECT().RecvMsg(gomock.Any()).Return(nil).AnyTimes()
				mockStream.EXPECT().SendMsg(gomock.Any()).Return(overloadedErr)

				return mockStream, nil
			})

		cc1 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node1:2135", NodeIDField: 1, StateField: state.Online,
		}
		cc2 := &mock.Conn{AddrField: "node2:2135", NodeIDField: 2, StateField: state.Online}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, policy.Info{}, nil)
		b.connectionsState.Store(s)

		streamCtx := BanOnOperationError(
			endpoint.WithNodeID(ctx, cc1.NodeIDField),
			Ydb.StatusIds_OVERLOADED,
		)
		stream, err := b.NewStream(streamCtx, &grpc.StreamDesc{}, "/test.Service/Stream")
		require.NoError(t, err)
		require.NotNil(t, stream)

		err = stream.SendMsg(nil)
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED))
		require.Equal(t, state.Banned, cc1.State())

		for range 10 {
			c, nextErr := b.nextConn(ctx)
			require.NoError(t, nextErr)
			require.Equal(t, cc2.AddrField, c.Endpoint().Address())
		}
	})

	t.Run("StreamRecvMsgErrorBansConnection", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		cc := mock.NewMockClientConnInterface(ctrl)
		mockStream := mock.NewMockClientStream(ctrl)
		cc.EXPECT().NewStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
				mockStream.EXPECT().Context().Return(ctx).AnyTimes()
				mockStream.EXPECT().Header().Return(nil, nil).AnyTimes()
				mockStream.EXPECT().Trailer().Return(nil).AnyTimes()
				mockStream.EXPECT().CloseSend().Return(nil).AnyTimes()
				mockStream.EXPECT().SendMsg(gomock.Any()).Return(nil).AnyTimes()
				mockStream.EXPECT().RecvMsg(gomock.Any()).Return(overloadedErr)

				return mockStream, nil
			})

		cc1 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node1:2135", NodeIDField: 1, StateField: state.Online,
		}
		cc2 := &mock.Conn{AddrField: "node2:2135", NodeIDField: 2, StateField: state.Online}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.RemoveRef(ctx) }()

		b := &Balancer{
			driverConfig: cfg,
			pool:         pool,
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, policy.Info{}, nil)
		b.connectionsState.Store(s)

		streamCtx := BanOnOperationError(
			endpoint.WithNodeID(ctx, cc1.NodeIDField),
			Ydb.StatusIds_OVERLOADED,
		)
		stream, err := b.NewStream(streamCtx, &grpc.StreamDesc{}, "/test.Service/Stream")
		require.NoError(t, err)
		require.NotNil(t, stream)

		err = stream.RecvMsg(nil)
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED))
		require.Equal(t, state.Banned, cc1.State())

		for range 10 {
			c, nextErr := b.nextConn(ctx)
			require.NoError(t, nextErr)
			require.Equal(t, cc2.AddrField, c.Endpoint().Address())
		}
	})
}

type countingPool struct {
	counts map[endpoint.Key]int
	conns  map[endpoint.Key]conn.Conn
}

func newCountingPool() *countingPool {
	return &countingPool{
		counts: make(map[endpoint.Key]int),
		conns:  make(map[endpoint.Key]conn.Conn),
	}
}

func (p *countingPool) Get(e endpoint.Endpoint) conn.Conn {
	key := e.Key()
	c, ok := p.conns[key]
	if !ok {
		c = &mock.Conn{
			AddrField:   e.Address(),
			NodeIDField: e.NodeID(),
		}
		p.conns[key] = c
	}
	p.counts[key]++

	return c
}

func (p *countingPool) Put(ctx context.Context, cc conn.Conn) {
	p.counts[cc.Endpoint().Key()]--
}

func (p *countingPool) count(key endpoint.Key) int {
	return p.counts[key]
}

func requireConnKeys(t *testing.T, expected []endpoint.Endpoint, actual []conn.Conn) {
	t.Helper()

	require.Len(t, actual, len(expected))
	for i, e := range expected {
		require.Equal(t, e.Key(), actual[i].Endpoint().Key())
	}
}

func TestNextState(t *testing.T) {
	var (
		ctx        = t.Context()
		pool       = newCountingPool()
		quarantine []conn.Conn
		active     []conn.Conn

		a = endpoint.New("node-a:2135", endpoint.WithID(1))
		b = endpoint.New("node-b:2135", endpoint.WithID(2))
		c = endpoint.New("node-c:2135", endpoint.WithID(3))
	)

	// Discovery #1: [a, b, c] — first acquire, nothing to release from quarantine.
	quarantine, active = nextState(ctx, pool, quarantine, active, []endpoint.Endpoint{a, b, c})
	require.Empty(t, quarantine)
	requireConnKeys(t, []endpoint.Endpoint{a, b, c}, active)
	require.Equal(t, 1, pool.count(a.Key()))
	require.Equal(t, 1, pool.count(b.Key()))
	require.Equal(t, 1, pool.count(c.Key()))

	connA, connB, connC := active[0], active[1], active[2]

	// Discovery #2: same set — previous active moves to quarantine, Get bumps refs again.
	quarantine, active = nextState(ctx, pool, quarantine, active, []endpoint.Endpoint{a, b, c})
	requireConnKeys(t, []endpoint.Endpoint{a, b, c}, quarantine)
	requireConnKeys(t, []endpoint.Endpoint{a, b, c}, active)
	require.Same(t, connA, quarantine[0])
	require.Same(t, connB, quarantine[1])
	require.Same(t, connC, quarantine[2])
	require.Equal(t, 2, pool.count(a.Key()))
	require.Equal(t, 2, pool.count(b.Key()))
	require.Equal(t, 2, pool.count(c.Key()))

	// Discovery #3: drop c — release quarantine, c stays referenced only from new quarantine.
	quarantine, active = nextState(ctx, pool, quarantine, active, []endpoint.Endpoint{a, b})
	requireConnKeys(t, []endpoint.Endpoint{a, b, c}, quarantine)
	requireConnKeys(t, []endpoint.Endpoint{a, b}, active)
	require.Equal(t, 2, pool.count(a.Key()))
	require.Equal(t, 2, pool.count(b.Key()))
	require.Equal(t, 1, pool.count(c.Key()))

	// Discovery #4: same [a, b] — release full quarantine; c ref drops to zero.
	quarantine, active = nextState(ctx, pool, quarantine, active, []endpoint.Endpoint{a, b})
	requireConnKeys(t, []endpoint.Endpoint{a, b}, quarantine)
	requireConnKeys(t, []endpoint.Endpoint{a, b}, active)
	require.Equal(t, 2, pool.count(a.Key()))
	require.Equal(t, 2, pool.count(b.Key()))
	require.Equal(t, 0, pool.count(c.Key()))

	// Discovery #5: c returns — new Get for c, a/b keep elevated refs.
	quarantine, active = nextState(ctx, pool, quarantine, active, []endpoint.Endpoint{a, b, c})
	requireConnKeys(t, []endpoint.Endpoint{a, b}, quarantine)
	requireConnKeys(t, []endpoint.Endpoint{a, b, c}, active)
	require.Equal(t, 2, pool.count(a.Key()))
	require.Equal(t, 2, pool.count(b.Key()))
	require.Equal(t, 1, pool.count(c.Key()))
	require.Same(t, connC, active[2])

	// Discovery #6: cluster empty — active set moves to quarantine, one ref each.
	quarantine, active = nextState(ctx, pool, quarantine, active, nil)
	requireConnKeys(t, []endpoint.Endpoint{a, b, c}, quarantine)
	require.Empty(t, active)
	require.Equal(t, 1, pool.count(a.Key()))
	require.Equal(t, 1, pool.count(b.Key()))
	require.Equal(t, 1, pool.count(c.Key()))

	// Discovery #7: still empty — release quarantine, all refs reach zero.
	quarantine, active = nextState(ctx, pool, quarantine, active, nil)
	require.Empty(t, quarantine)
	require.Empty(t, active)
	require.Equal(t, 0, pool.count(a.Key()))
	require.Equal(t, 0, pool.count(b.Key()))
	require.Equal(t, 0, pool.count(c.Key()))
}

func TestNextStateClosedPool(t *testing.T) {
	ctx := context.Background()
	pool := conn.NewPool(ctx, config.New())
	require.NoError(t, pool.RemoveRef(ctx))

	newQuarantine, newActive := nextState(ctx, pool, nil, nil, []endpoint.Endpoint{
		endpoint.New("node:2135", endpoint.WithID(1)),
	})

	require.Empty(t, newQuarantine)
	require.Empty(t, newActive)
}

func TestNewReturnsDiscoveryStartError(t *testing.T) {
	ctx := t.Context()
	expectedErr := errors.New("credentials failed")
	srv := startDynamicDiscoveryServer(t, []uint32{1})
	cfg := config.New(
		config.WithEndpoint(srv.endpoint()),
		config.WithDatabase("/local"),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		config.WithCredentials(errorCredentials{err: expectedErr}),
	)
	pool := conn.NewPool(ctx, cfg)
	t.Cleanup(func() {
		require.NoError(t, pool.RemoveRef(ctx))
	})

	balancer, err := New(ctx, cfg, pool)

	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, balancer)
}

func TestClusterDiscoveryAttemptReturnsLocalDCDetectorError(t *testing.T) {
	expectedErr := errors.New("local DC detection failed")
	p := policy.PreferNearestDC(
		policy.Policy{}, "LocalDC", func(policy.Info, endpoint.Info) bool { return true },
	)
	balancer := &Balancer{
		driverConfig: config.New(),
		policy:       p,
		discover: func(context.Context, *grpc.ClientConn) ([]endpoint.Endpoint, string, error) {
			return []endpoint.Endpoint{endpoint.New("node:2135")}, "", nil
		},
		localDCDetector: func(context.Context, []endpoint.Endpoint) (string, error) {
			return "", expectedErr
		},
	}

	err := balancer.clusterDiscoveryAttempt(t.Context(), nil)

	require.ErrorIs(t, err, expectedErr)
}

func TestPolicyUsesFreshDiscoveryEndpointBeforePoolGet(t *testing.T) {
	ctx := context.Background()
	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	p := policy.Prefer(
		policy.Policy{}, "PreferredLocation",
		func(_ policy.Info, candidate endpoint.Info) bool {
			return candidate.Location() == "preferred"
		},
	)
	balancer := &Balancer{
		driverConfig: cfg,
		policy:       p,
		pool:         pool,
	}
	t.Cleanup(func() {
		require.NoError(t, balancer.Close(ctx))
		require.NoError(t, pool.RemoveRef(ctx))
	})

	first := locationEndpoints("preferred", "fallback")
	balancer.applyDiscoveredEndpoints(ctx, first, "")
	reused := balancer.connections().elector.connections[first[1].Key()]
	require.NotNil(t, reused)

	second := locationEndpoints("fallback", "preferred")
	balancer.applyDiscoveredEndpoints(ctx, second, "")

	selected, err := balancer.nextConn(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), selected.Endpoint().NodeID())
	require.Same(t, reused, selected, "the pool must reuse the existing connection wrapper")
	require.Equal(t, "fallback", selected.Endpoint().Location(),
		"selection must use the fresh discovery endpoint rather than attributes retained by the pooled connection",
	)
}

func TestDiscoveryReuseIPAndHostName(t *testing.T) {
	ctx := t.Context()
	cfg := config.New()
	discovered := mock.Endpoint{
		AddrField: "::1:123", NodeIDField: 1, OverrideHostField: "dyn-node-1.svc.cluster.local",
	}
	balancer := &Balancer{
		driverConfig: cfg,
		policy:       cfg.Balancer(),
		pool:         conn.NewPool(ctx, cfg),
		discover: func(context.Context, *grpc.ClientConn) ([]endpoint.Endpoint, string, error) {
			copy := discovered

			return []endpoint.Endpoint{&copy}, "", nil
		},
	}
	t.Cleanup(func() { require.NoError(t, balancer.pool.RemoveRef(ctx)) })

	check := func() {
		require.NoError(t, balancer.clusterDiscoveryAttempt(ctx, nil))
		selected, err := balancer.nextConn(ctx)
		require.NoError(t, err)
		require.Equal(t, discovered.AddrField, selected.Endpoint().Address())
		require.Equal(t, discovered.NodeIDField, selected.Endpoint().NodeID())
		require.Equal(t, discovered.OverrideHostField, selected.Endpoint().OverrideHost())
	}

	check()
	discovered.NodeIDField = 2
	check()
	discovered.OverrideHostField = "dyn-node-2.svc.cluster.local"
	check()
}

func locationEndpoints(first, second string) []endpoint.Endpoint {
	return []endpoint.Endpoint{
		endpoint.New("node-1", endpoint.WithID(1), endpoint.WithLocation(first)),
		endpoint.New("node-2", endpoint.WithID(2), endpoint.WithLocation(second)),
	}
}

type errorCredentials struct {
	err error
}

func (c errorCredentials) Token(context.Context) (string, error) {
	return "", c.err
}

func TestNextAvailableConnContinuesWithLatestSnapshot(t *testing.T) {
	replacement := &mock.Conn{
		AddrField:   "replacement:2135",
		NodeIDField: 2,
		StateField:  state.Online,
	}
	next := newConnectionsStateWithPolicy(
		[]conn.Conn{replacement}, policy.Policy{}, policy.Info{}, nil,
	)
	balancer := &Balancer{}
	staleBase := &mock.Conn{
		AddrField:   "stale:2135",
		NodeIDField: 1,
		StateField:  state.Online,
	}
	stale := &snapshotSwappingConn{
		Conn:      staleBase,
		balancer:  balancer,
		nextState: next,
	}
	previous := newConnectionsStateWithPolicy(
		[]conn.Conn{stale}, policy.Policy{}, policy.Info{}, nil,
	)
	balancer.connectionsState.Store(previous)
	stale.armed = true

	selected, failedCount := balancer.nextAvailableConn(t.Context(), previous)

	require.Same(t, replacement, selected)
	require.Equal(t, 1, failedCount)
	require.Same(t, next, balancer.connections())
}

func TestNextAvailableConnExtendsAttemptsForLargerSnapshot(t *testing.T) {
	firstUnavailable := &stateSequenceConn{
		Conn: &mock.Conn{AddrField: "first-unavailable:2135", NodeIDField: 2, StateField: state.Destroyed},
		states: []state.State{
			state.Online,
			state.Destroyed,
		},
	}
	secondUnavailable := &stateSequenceConn{
		Conn: &mock.Conn{AddrField: "second-unavailable:2135", NodeIDField: 3, StateField: state.Destroyed},
		states: []state.State{
			state.Online,
			state.Online,
			state.Destroyed,
		},
	}
	available := &mock.Conn{
		AddrField:   "available:2135",
		NodeIDField: 4,
		StateField:  state.Online,
	}
	next := newConnectionsStateWithPolicyAndRand(
		[]conn.Conn{firstUnavailable, secondUnavailable, available},
		policy.Policy{}, policy.Info{}, nil, userAPITestRand{},
	)
	balancer := &Balancer{}
	stale := &snapshotSwappingConn{
		Conn:      &mock.Conn{AddrField: "stale:2135", NodeIDField: 1, StateField: state.Online},
		balancer:  balancer,
		nextState: next,
	}
	previous := newConnectionsStateWithPolicy(
		[]conn.Conn{stale}, policy.Policy{}, policy.Info{}, nil,
	)
	balancer.connectionsState.Store(previous)
	stale.armed = true

	selected, failedCount := balancer.nextAvailableConn(t.Context(), previous)

	require.Same(t, available, selected)
	require.Equal(t, 2, failedCount)
	require.Same(t, next, balancer.connections())
}

func TestNextAvailableConnStopsWhenBalancerClosesDuringSelection(t *testing.T) {
	balancer := &Balancer{}
	staleBase := &mock.Conn{
		AddrField:   "stale:2135",
		NodeIDField: 1,
		StateField:  state.Online,
	}
	stale := &snapshotSwappingConn{
		Conn:     staleBase,
		balancer: balancer,
	}
	previous := newConnectionsStateWithPolicy(
		[]conn.Conn{stale}, policy.Policy{}, policy.Info{}, nil,
	)
	balancer.connectionsState.Store(previous)
	stale.armed = true

	selected, failedCount := balancer.nextAvailableConn(t.Context(), previous)

	require.Nil(t, selected)
	require.Equal(t, 1, failedCount)
	require.Nil(t, balancer.connections())
}

func TestNextAvailableConnStopsWhenElectionSnapshotIsEmpty(t *testing.T) {
	connections := newConnectionsStateWithPriorities(nil, nil, nil, nil)
	balancer := &Balancer{}
	balancer.connectionsState.Store(connections)

	selected, failedCount := balancer.nextAvailableConn(t.Context(), connections)

	require.Nil(t, selected)
	require.Zero(t, failedCount)
}

func TestNextAvailableConnStopsWhenElectionSnapshotBecomesEmpty(t *testing.T) {
	connection := &mock.Conn{AddrField: "available:2135", StateField: state.Online}
	connections := newConnectionsStateWithPolicy(
		[]conn.Conn{connection}, policy.Policy{}, policy.Info{}, nil,
	)
	balancer := &Balancer{}
	balancer.connectionsState.Store(connections)
	ctx := &clearElectionContext{elector: connections.elector}

	selected, failedCount := balancer.nextAvailableConn(ctx, connections)

	require.Nil(t, selected)
	require.Zero(t, failedCount)
}

func TestNextConnReturnsNoEndpointsWhenElectionSnapshotIsEmpty(t *testing.T) {
	connection := &mock.Conn{AddrField: "destroyed:2135", StateField: state.Destroyed}
	connections := newConnectionsStateWithPriorities(
		[]conn.Conn{connection},
		[]policy.EndpointPriority{{Key: connection.Endpoint().Key()}},
		nil,
		nil,
	)
	balancer := &Balancer{driverConfig: config.New()}
	balancer.connectionsState.Store(connections)

	selected, err := balancer.nextConn(t.Context())

	require.Nil(t, selected)
	require.ErrorIs(t, err, ErrNoEndpoints)
	require.NotContains(t, err.Error(), "after 0 attempts")
}

func TestNextAvailableConnStopsWhenContextIsCanceled(t *testing.T) {
	connection := &mock.Conn{AddrField: "available:2135", StateField: state.Online}
	connections := newConnectionsStateWithPolicy(
		[]conn.Conn{connection}, policy.Policy{}, policy.Info{}, nil,
	)
	balancer := &Balancer{}
	balancer.connectionsState.Store(connections)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	selected, failedCount := balancer.nextAvailableConn(ctx, connections)

	require.Nil(t, selected)
	require.Zero(t, failedCount)
}

func TestNextConnReturnsCanceledContext(t *testing.T) {
	connection := &mock.Conn{AddrField: "available:2135", StateField: state.Online}
	balancer := &Balancer{driverConfig: config.New()}
	balancer.connectionsState.Store(newConnectionsStateWithPolicy(
		[]conn.Conn{connection}, policy.Policy{}, policy.Info{}, nil,
	))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	selected, err := balancer.nextConn(ctx)

	require.Nil(t, selected)
	require.ErrorIs(t, err, context.Canceled)
}

func TestNextConnReturnsContextCanceledDuringSelection(t *testing.T) {
	connection := &mock.Conn{AddrField: "available:2135", StateField: state.Online}
	balancer := &Balancer{driverConfig: config.New()}
	balancer.connectionsState.Store(newConnectionsStateWithPolicy(
		[]conn.Conn{connection}, policy.Policy{}, policy.Info{}, nil,
	))
	ctx := &cancelAfterFirstCheckContext{}

	selected, err := balancer.nextConn(ctx)

	require.Nil(t, selected)
	require.ErrorIs(t, err, context.Canceled)
}

type cancelAfterFirstCheckContext struct {
	checks int
}

func (*cancelAfterFirstCheckContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (*cancelAfterFirstCheckContext) Done() <-chan struct{} {
	return nil
}

func (c *cancelAfterFirstCheckContext) Err() error {
	c.checks++
	if c.checks > 1 {
		return context.Canceled
	}

	return nil
}

func (*cancelAfterFirstCheckContext) Value(any) any {
	return nil
}

type snapshotSwappingConn struct {
	conn.Conn

	balancer  *Balancer
	nextState *connectionsState
	armed     bool
	swapped   bool
}

func (c *snapshotSwappingConn) State() state.State {
	if c.armed && !c.swapped {
		c.swapped = true
		c.balancer.connectionsState.Store(c.nextState)

		return state.Destroyed
	}

	return c.Conn.State()
}

type stateSequenceConn struct {
	conn.Conn

	states []state.State
	index  int
}

func (c *stateSequenceConn) State() state.State {
	if c.index < len(c.states) {
		connectionState := c.states[c.index]
		c.index++

		return connectionState
	}

	return c.Conn.State()
}

func TestWithNodeIDBypassesSelectionPolicies(t *testing.T) {
	connections := []conn.Conn{
		userBalancerConn(1, "preferred", state.Online),
		userBalancerConn(2, "excluded", state.Online),
	}
	balancer := userConfiguredBalancer(
		config.WithBalancer(userBalancers.PreferLocations(
			userBalancers.RandomChoice(), "preferred",
		)),
		connections,
		"",
	)

	selected, err := balancer.nextConn(userBalancers.WithNodeID(t.Context(), 2))
	require.NoError(t, err)
	require.Same(t, connections[1], selected)
}

func TestPinnedNodeIDDoesNotFallbackToAnotherConnection(t *testing.T) {
	balancer := userConfiguredBalancer(
		config.WithBalancer(userBalancers.RandomChoice()),
		[]conn.Conn{userBalancerConn(1, "available", state.Online)},
		"",
	)

	ctx := endpoint.WithNodeID(t.Context(), 2, endpoint.WithFallback(false))
	selected, err := balancer.nextConn(ctx)
	require.ErrorIs(t, err, ErrNoEndpoints)
	require.Nil(t, selected)
}

func TestBalancerHandlesBanAndUnban(t *testing.T) {
	preferred := userBalancerConn(1, "preferred", state.Online)
	fallback := userBalancerConn(2, "fallback", state.Online)
	option := config.WithBalancer(userBalancers.PreferLocations(
		userBalancers.RandomChoice(), "preferred",
	))
	balancer := userConfiguredBalancer(option, []conn.Conn{preferred, fallback}, "")

	selected, err := balancer.nextConn(t.Context())
	require.NoError(t, err)
	require.Same(t, preferred, selected)

	preferred.Ban(t.Context())
	selected, err = balancer.nextConn(t.Context())
	require.NoError(t, err)
	require.Same(t, fallback, selected)

	preferred.Unban(t.Context())
	// Discovery unbans pooled connections before it publishes a fresh election snapshot.
	balancer.connections().elector.Refresh()
	selected, err = balancer.nextConn(t.Context())
	require.NoError(t, err)
	require.Same(t, preferred, selected)
}

func TestUserBalancerConfigurations(t *testing.T) {
	tests := []struct {
		name         string
		option       config.Option
		selfLocation string
		connections  []conn.Conn
		allowed      map[uint32]struct{}
	}{
		{
			name:   "random choice",
			option: config.WithBalancer(userBalancers.RandomChoice()),
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Online),
				userBalancerConn(2, "b", state.Online),
			},
			allowed: nodeIDSet(1, 2),
		},
		{
			name:   "single connection",
			option: config.WithBalancer(userBalancers.SingleConn()),
			connections: []conn.Conn{
				userBalancerConn(1, "configured", state.Online),
			},
			allowed: nodeIDSet(1),
		},
		{
			name: "prefer nearest dc",
			option: config.WithBalancer(userBalancers.PreferNearestDC(
				userBalancers.RandomChoice(),
			)),
			selfLocation: "a",
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Online),
				userBalancerConn(2, "b", state.Online),
			},
			allowed: nodeIDSet(1),
		},
		{
			name: "prefer nearest dc cascades",
			option: config.WithBalancer(userBalancers.PreferNearestDC(
				userBalancers.RandomChoice(),
			)),
			selfLocation: "a",
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Banned),
				userBalancerConn(2, "b", state.Online),
			},
			allowed: nodeIDSet(2),
		},
		{
			name: "prefer locations",
			option: config.WithBalancer(userBalancers.PreferLocations(
				userBalancers.RandomChoice(), "a", "c",
			)),
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Online),
				userBalancerConn(2, "b", state.Online),
				userBalancerConn(3, "c", state.Online),
			},
			allowed: nodeIDSet(1, 3),
		},
		{
			name: "prefer locations cascades",
			option: config.WithBalancer(userBalancers.PreferLocations(
				userBalancers.RandomChoice(), "a",
			)),
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Banned),
				userBalancerConn(2, "b", state.Online),
			},
			allowed: nodeIDSet(2),
		},
		{
			name: "custom preference",
			option: config.WithBalancer(userBalancers.Prefer(
				userBalancers.RandomChoice(),
				func(endpoint userBalancers.Endpoint) bool {
					return endpoint.NodeID()%2 == 0
				},
			)),
			connections: []conn.Conn{
				userBalancerConn(1, "a", state.Online),
				userBalancerConn(2, "b", state.Online),
				userBalancerConn(3, "c", state.Online),
			},
			allowed: nodeIDSet(2),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			balancer := userConfiguredBalancer(test.option, test.connections, test.selfLocation)
			selectedNodeIDs := make(map[uint32]struct{}, len(test.allowed))

			for index := range len(test.allowed) {
				rand := userAPITestRand{index: index}
				balancer.connectionsState.Load().elector.rand = rand
				selected, err := balancer.nextConn(t.Context())
				require.NoError(t, err)
				selectedNodeIDs[selected.Endpoint().NodeID()] = struct{}{}
			}

			require.Equal(t, test.allowed, selectedNodeIDs)
		})
	}
}

func userConfiguredBalancer(option config.Option, connections []conn.Conn, selfLocation string) *Balancer {
	cfg := config.New(option)
	balancer := &Balancer{
		driverConfig: cfg,
		policy:       cfg.Balancer(),
	}
	balancer.connectionsState.Store(newConnectionsStateWithPolicy(
		connections,
		balancer.policy,
		policy.Info{SelfLocation: selfLocation},
		nil,
	))

	return balancer
}

func userBalancerConn(nodeID uint32, location string, connectionState state.State) conn.Conn {
	return &mock.Conn{
		AddrField:     location,
		LocationField: location,
		NodeIDField:   nodeID,
		StateField:    connectionState,
	}
}

func nodeIDSet(nodeIDs ...uint32) map[uint32]struct{} {
	result := make(map[uint32]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		result[nodeID] = struct{}{}
	}

	return result
}

type userAPITestRand struct {
	index int
}

func (userAPITestRand) Int64(int64) int64 {
	return 0
}

func (r userAPITestRand) Int(maximum int) int {
	return r.index % maximum
}

func (userAPITestRand) Shuffle(int, func(int, int)) {}

func BenchmarkNextConn(b *testing.B) {
	tests := []struct {
		name        string
		nodeCount   int
		bannedCount int
		balancer    func() config.Option
	}{
		{
			name:      "RandomChoice",
			nodeCount: 1,
			balancer: func() config.Option {
				return config.WithBalancer(userBalancers.RandomChoice())
			},
		},
		{
			name:      "RandomChoice",
			nodeCount: 10,
			balancer: func() config.Option {
				return config.WithBalancer(userBalancers.RandomChoice())
			},
		},
		{
			name:      "RandomChoice",
			nodeCount: 1000,
			balancer: func() config.Option {
				return config.WithBalancer(userBalancers.RandomChoice())
			},
		},
		{
			name:      "Prefer",
			nodeCount: 1000,
			balancer: func() config.Option {
				return config.WithBalancer(userBalancers.Prefer(
					userBalancers.RandomChoice(),
					func(candidate userBalancers.Endpoint) bool {
						return candidate.NodeID()%2 == 0
					},
				))
			},
		},
		{
			name:        "RandomChoiceHalfBanned",
			nodeCount:   1000,
			bannedCount: 500,
			balancer: func() config.Option {
				return config.WithBalancer(userBalancers.RandomChoice())
			},
		},
	}

	for _, test := range tests {
		b.Run(test.name+"/"+strconv.Itoa(test.nodeCount), func(b *testing.B) {
			benchmarkNextConn(b, test.nodeCount, test.bannedCount, test.balancer())
		})
	}
}

func benchmarkNextConn(b *testing.B, nodeCount, bannedCount int, balancerOption config.Option) {
	nodeIDs := make([]uint32, nodeCount)
	for i := range nodeIDs {
		nodeIDs[i] = uint32(i + 1)
	}
	discovery := startDynamicDiscoveryServer(b, nodeIDs)
	ctx := context.Background()
	cfg := config.New(
		config.WithEndpoint(discovery.endpoint()),
		config.WithDatabase("/benchmark"),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		balancerOption,
	)
	pool := conn.NewPool(ctx, cfg)
	balancer, err := New(ctx, cfg, pool)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := balancer.Close(ctx); err != nil {
			b.Error(err)
		}
		if err := pool.RemoveRef(ctx); err != nil {
			b.Error(err)
		}
	})
	connections := balancer.connections().All()
	for i := range min(bannedCount, len(connections)) {
		connections[i].Ban(ctx)
	}
	for range nodeCount {
		if _, err = balancer.nextConn(ctx); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		nextConnBenchmarkSink, err = balancer.nextConn(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

var nextConnBenchmarkSink conn.Conn

type dynamicDiscoveryServer struct {
	listener   net.Listener
	grpcServer *grpc.Server

	mu      sync.RWMutex
	nodeIDs []uint32
	host    string
	port    uint32

	activeConns atomic.Int64
}

type dynamicDiscoveryService struct {
	Ydb_Discovery_V1.UnimplementedDiscoveryServiceServer

	srv *dynamicDiscoveryServer
}

func (s *dynamicDiscoveryService) ListEndpoints(
	_ context.Context,
	_ *Ydb_Discovery.ListEndpointsRequest,
) (*Ydb_Discovery.ListEndpointsResponse, error) {
	endpoints := s.srv.currentEndpoints()

	return &Ydb_Discovery.ListEndpointsResponse{
		Operation: discoveryOperationOK(&Ydb_Discovery.ListEndpointsResult{
			Endpoints: endpoints,
		}),
	}, nil
}

func (s *dynamicDiscoveryService) WhoAmI(
	_ context.Context,
	_ *Ydb_Discovery.WhoAmIRequest,
) (*Ydb_Discovery.WhoAmIResponse, error) {
	return &Ydb_Discovery.WhoAmIResponse{
		Operation: discoveryOperationOK(&emptypb.Empty{}),
	}, nil
}

func (s *dynamicDiscoveryServer) currentEndpoints() []*Ydb_Discovery.EndpointInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodeIDs := append([]uint32(nil), s.nodeIDs...)

	return mockDiscoveryEndpoints(s.host, s.port, nodeIDs)
}

func (s *dynamicDiscoveryServer) setNodeIDs(nodeIDs []uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeIDs = append([]uint32(nil), nodeIDs...)
}

func (s *dynamicDiscoveryServer) endpoint() string {
	return net.JoinHostPort(s.host, strconv.FormatUint(uint64(s.port), 10))
}

func (s *dynamicDiscoveryServer) Close() {
	s.grpcServer.Stop()
	_ = s.listener.Close()
}

func (s *dynamicDiscoveryServer) activeGRPCConns() int64 {
	return s.activeConns.Load()
}

type serverConnStats struct {
	active *atomic.Int64
}

func (h *serverConnStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *serverConnStats) HandleRPC(context.Context, stats.RPCStats) {}

func (h *serverConnStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *serverConnStats) HandleConn(_ context.Context, connectionStats stats.ConnStats) {
	switch connectionStats.(type) {
	case *stats.ConnBegin:
		h.active.Add(1)
	case *stats.ConnEnd:
		h.active.Add(-1)
	}
}

func startDynamicDiscoveryServer(tb testing.TB, nodeIDs []uint32) *dynamicDiscoveryServer {
	tb.Helper()

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(tb.Context(), "tcp", "127.0.0.1:0")
	require.NoError(tb, err)

	host, portString, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(tb, err)

	parsedPort, err := strconv.ParseUint(portString, 10, 32)
	require.NoError(tb, err)

	server := &dynamicDiscoveryServer{
		listener: listener,
		host:     host,
		port:     uint32(parsedPort),
		nodeIDs:  append([]uint32(nil), nodeIDs...),
	}

	statsHandler := &serverConnStats{active: &server.activeConns}
	server.grpcServer = grpc.NewServer(grpc.StatsHandler(statsHandler))

	Ydb_Discovery_V1.RegisterDiscoveryServiceServer(
		server.grpcServer,
		&dynamicDiscoveryService{srv: server},
	)

	go func() {
		_ = server.grpcServer.Serve(listener)
	}()

	tb.Cleanup(server.Close)

	require.Eventually(tb, func() bool {
		var dialer net.Dialer
		connection, dialErr := dialer.DialContext(tb.Context(), "tcp", listener.Addr().String())
		if dialErr != nil {
			return false
		}
		_ = connection.Close()

		return true
	}, time.Second, 10*time.Millisecond)

	return server
}

func mockDiscoveryEndpoints(host string, port uint32, nodeIDs []uint32) []*Ydb_Discovery.EndpointInfo {
	endpoints := make([]*Ydb_Discovery.EndpointInfo, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		endpoints[i] = &Ydb_Discovery.EndpointInfo{
			Address:    host,
			Port:       port,
			LoadFactor: 0,
			Ssl:        false,
			NodeId:     nodeID,
			IpV4:       []string{host},
		}
	}

	return endpoints
}

func discoveryOperationOK(message proto.Message) *Ydb_Operations.Operation {
	result := &anypb.Any{}
	if err := result.MarshalFrom(message); err != nil {
		panic(err)
	}

	return &Ydb_Operations.Operation{
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
		Result: result,
	}
}

type connLifeEvents struct {
	mu     sync.Mutex
	dialed map[uint32]int
	parked map[uint32]int
	closed map[uint32]int
}

func newConnLifeEvents() *connLifeEvents {
	return &connLifeEvents{
		dialed: make(map[uint32]int),
		parked: make(map[uint32]int),
		closed: make(map[uint32]int),
	}
}

func (e *connLifeEvents) driverTrace() *trace.Driver {
	return &trace.Driver{
		OnConnDial: func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
			nodeID := info.Endpoint.NodeID()

			return func(done trace.DriverConnDialDoneInfo) {
				if done.Error != nil {
					return
				}
				e.mu.Lock()
				e.dialed[nodeID]++
				e.mu.Unlock()
			}
		},
		OnConnClose: func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			nodeID := info.Endpoint.NodeID()

			return func(trace.DriverConnCloseDoneInfo) {
				e.mu.Lock()
				e.closed[nodeID]++
				e.mu.Unlock()
			}
		},
		OnConnPark: func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			nodeID := info.Endpoint.NodeID()

			return func(done trace.DriverConnParkDoneInfo) {
				if done.Error != nil {
					return
				}
				e.mu.Lock()
				e.parked[nodeID]++
				e.mu.Unlock()
			}
		},
	}
}

func (e *connLifeEvents) dialedCount(nodeID uint32) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.dialed[nodeID]
}

func (e *connLifeEvents) closedCount(nodeID uint32) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.closed[nodeID]
}

func (e *connLifeEvents) parkedCount(nodeID uint32) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.parked[nodeID]
}

func dialWhoAmI(tb testing.TB, balancer *Balancer, nodeID uint32) {
	tb.Helper()

	ctx := endpoint.WithNodeID(tb.Context(), nodeID)
	reply := &Ydb_Discovery.WhoAmIResponse{}

	err := balancer.Invoke(
		ctx,
		Ydb_Discovery_V1.DiscoveryService_WhoAmI_FullMethodName,
		&Ydb_Discovery.WhoAmIRequest{},
		reply,
	)
	require.NoError(tb, err)
}

func connByNodeID(balancer *Balancer, nodeID uint32) conn.Conn {
	for _, connection := range balancer.connections().All() {
		if connection.Endpoint().NodeID() == nodeID {
			return connection
		}
	}

	return nil
}

func activeNodeIDs(balancer *Balancer) []uint32 {
	connections := balancer.connections().All()
	ids := make([]uint32, len(connections))
	for i, connection := range connections {
		ids[i] = connection.Endpoint().NodeID()
	}

	return ids
}

func connInQuarantine(balancer *Balancer, nodeID uint32) conn.Conn {
	if balancerState := balancer.connectionsState.Load(); balancerState != nil {
		for _, connection := range balancerState.quarantine {
			if connection.Endpoint().NodeID() == nodeID {
				return connection
			}
		}
	}

	return nil
}

// TestBalancerDiscoveryDropClosesGRPC verifies end-to-end that a node removed from
// ListEndpoints eventually closes its pooled gRPC connection after the quarantine
// cycle (two discovery rounds after the drop), and Balancer.Close closes the rest.
func TestBalancerDiscoveryDropClosesGRPC(t *testing.T) {
	const (
		node1 uint32 = 1
		node2 uint32 = 2
	)

	ctx := t.Context()
	server := startDynamicDiscoveryServer(t, []uint32{node1, node2})
	events := newConnLifeEvents()

	cfg := config.New(
		config.WithEndpoint(server.endpoint()),
		config.WithDatabase("/local"),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		config.WithTrace(*events.driverTrace()),
		config.WithBalancer(userBalancers.RandomChoice()),
	)

	pool := conn.NewPool(ctx, cfg)
	defer func() {
		require.NoError(t, pool.RemoveRef(ctx))
	}()

	balancer, err := New(ctx, cfg, pool, discoveryConfig.WithInterval(0))
	require.NoError(t, err)

	require.ElementsMatch(t, []uint32{node1, node2}, activeNodeIDs(balancer))

	dialWhoAmI(t, balancer, node1)
	dialWhoAmI(t, balancer, node2)

	require.Equal(t, 1, events.dialedCount(node1))
	require.Equal(t, 1, events.dialedCount(node2))
	require.GreaterOrEqual(t, server.activeGRPCConns(), int64(2))

	node2Conn := connByNodeID(balancer, node2)
	require.NotNil(t, node2Conn)
	require.Equal(t, state.Online, node2Conn.State())

	// Discovery #2: same cluster — quarantine cycle, refs increment.
	require.NoError(t, balancer.clusterDiscoveryAttemptWithDial(ctx))
	require.ElementsMatch(t, []uint32{node1, node2}, activeNodeIDs(balancer))
	require.Equal(t, 0, events.closedCount(node2))

	// Discovery #3: node 2 disappears — moved to quarantine, gRPC still alive.
	server.setNodeIDs([]uint32{node1})
	require.NoError(t, balancer.clusterDiscoveryAttemptWithDial(ctx))
	require.Equal(t, []uint32{node1}, activeNodeIDs(balancer))
	require.Equal(t, 0, events.closedCount(node2), "gRPC must not close on first discovery after drop")

	quarantined := connInQuarantine(balancer, node2)
	require.NotNil(t, quarantined, "dropped node must remain in quarantine")
	require.Same(t, node2Conn, quarantined)

	// Discovery #4: same cluster — quarantine released, dropped node gRPC must close.
	require.NoError(t, balancer.clusterDiscoveryAttemptWithDial(ctx))
	require.Equal(t, []uint32{node1}, activeNodeIDs(balancer))

	require.Eventually(t, func() bool {
		return events.closedCount(node2) == 1 && node2Conn.State() == state.Destroyed
	}, time.Second, 10*time.Millisecond, "node removed from discovery must close gRPC connection")

	require.Equal(t, 0, events.closedCount(node1))

	// Balancer.Close: release active + quarantine, remaining pooled conns must close.
	node1Conn := connByNodeID(balancer, node1)
	require.NotNil(t, node1Conn)
	require.Equal(t, 0, events.closedCount(node1))

	require.NoError(t, balancer.Close(ctx))

	require.Eventually(t, func() bool {
		return events.closedCount(node1) == 1 && node1Conn.State() == state.Destroyed
	}, time.Second, 10*time.Millisecond, "balancer close must close remaining active connections")

	require.Equal(t, 1, events.closedCount(node2))
}

func TestBalancerDiscoveryDropDestroysParkedConnection(t *testing.T) {
	const (
		node1 uint32 = 1
		node2 uint32 = 2
		ttl          = 50 * time.Millisecond
	)

	ctx := t.Context()
	server := startDynamicDiscoveryServer(t, []uint32{node1, node2})
	events := newConnLifeEvents()

	cfg := config.New(
		config.WithEndpoint(server.endpoint()),
		config.WithDatabase("/local"),
		config.WithConnectionTTL(ttl),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		config.WithTrace(*events.driverTrace()),
		config.WithBalancer(userBalancers.RandomChoice()),
	)

	pool := conn.NewPool(ctx, cfg)
	defer func() {
		require.NoError(t, pool.RemoveRef(ctx))
	}()

	balancer, err := New(ctx, cfg, pool, discoveryConfig.WithInterval(0))
	require.NoError(t, err)

	dialWhoAmI(t, balancer, node2)
	node2Conn := connByNodeID(balancer, node2)
	require.NotNil(t, node2Conn)

	require.Eventually(t, func() bool {
		return node2Conn.State() == state.Offline && events.parkedCount(node2) == 1
	}, time.Second, 10*time.Millisecond, "connection TTL must park the node transport")
	require.Equal(t, 0, events.closedCount(node2), "parking must preserve the pooled wrapper")

	// Discovery #2: keep both nodes and establish the quarantine generation.
	require.NoError(t, balancer.clusterDiscoveryAttemptWithDial(ctx))
	require.Same(t, node2Conn, connByNodeID(balancer, node2))
	require.Equal(t, state.Offline, node2Conn.State())

	// Discovery #3: node 2 disappears but remains referenced by quarantine.
	server.setNodeIDs([]uint32{node1})
	require.NoError(t, balancer.clusterDiscoveryAttemptWithDial(ctx))
	require.Same(t, node2Conn, connInQuarantine(balancer, node2))
	require.Equal(t, state.Offline, node2Conn.State())
	require.Equal(t, 0, events.closedCount(node2))

	// Discovery #4: release quarantine and destroy the already parked wrapper.
	require.NoError(t, balancer.clusterDiscoveryAttemptWithDial(ctx))
	require.Eventually(t, func() bool {
		return node2Conn.State() == state.Destroyed && events.closedCount(node2) == 1
	}, time.Second, 10*time.Millisecond, "Discovery removal must destroy a parked connection")
	require.Equal(t, 1, events.parkedCount(node2))
	require.Equal(t, 1, events.dialedCount(node2))

	require.NoError(t, balancer.Close(ctx))
}

func TestBalancerConnectionTTLParksTransportsAfterNetworkLoss(t *testing.T) {
	const (
		nodeCount = 16
		ttl       = 50 * time.Millisecond
	)

	nodeIDs := make([]uint32, nodeCount)
	for i := range nodeIDs {
		nodeIDs[i] = uint32(i + 1)
	}

	ctx := t.Context()
	server := startDynamicDiscoveryServer(t, nodeIDs)
	events := newConnLifeEvents()

	cfg := config.New(
		config.WithEndpoint(server.endpoint()),
		config.WithDatabase("/local"),
		config.WithConnectionTTL(ttl),
		config.WithDialTimeout(ttl),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		config.WithTrace(*events.driverTrace()),
		config.WithBalancer(userBalancers.RandomChoice()),
	)

	pool := conn.NewPool(ctx, cfg)
	defer func() {
		require.NoError(t, pool.RemoveRef(ctx))
	}()

	balancer, err := New(ctx, cfg, pool, discoveryConfig.WithInterval(0))
	require.NoError(t, err)

	dialWhoAmI(t, balancer, nodeIDs[0])
	server.Close()

	for _, nodeID := range nodeIDs[1:] {
		callCtx, cancel := context.WithTimeout(endpoint.WithNodeID(ctx, nodeID), ttl)
		reply := &Ydb_Discovery.WhoAmIResponse{}
		err = balancer.Invoke(
			callCtx,
			Ydb_Discovery_V1.DiscoveryService_WhoAmI_FullMethodName,
			&Ydb_Discovery.WhoAmIRequest{},
			reply,
		)
		cancel()
		require.Error(t, err)
	}

	for _, nodeID := range nodeIDs {
		require.Equal(t, 1, events.dialedCount(nodeID))
	}

	require.Eventually(t, func() bool {
		for _, nodeID := range nodeIDs {
			connection := connByNodeID(balancer, nodeID)
			if connection == nil || connection.State() != state.Offline || events.parkedCount(nodeID) != 1 {
				return false
			}
		}

		return true
	}, 5*time.Second, 10*time.Millisecond)

	require.ElementsMatch(t, nodeIDs, activeNodeIDs(balancer),
		"parking must keep Discovery wrappers in the balancer",
	)
	require.NoError(t, balancer.Close(ctx))

	for _, nodeID := range nodeIDs {
		require.Equal(t, 1, events.closedCount(nodeID))
	}
}

type clearElectionContext struct {
	elector *endpointElector
}

func (*clearElectionContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (*clearElectionContext) Done() <-chan struct{} {
	return nil
}

func (c *clearElectionContext) Err() error {
	c.elector.snapshot.Store(&electionSnapshot{})

	return nil
}

func (*clearElectionContext) Value(any) any {
	return nil
}
