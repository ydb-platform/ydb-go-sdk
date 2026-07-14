package balancer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
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
		driverConfig:   cfg,
		pool:           pool,
		balancerConfig: balancerConfig.Config{},
	}

	initial := newConnectionsState(nil,
		b.balancerConfig.Filter, balancerConfig.Info{}, b.balancerConfig.AllowFallback, nil,
	)
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

type allowNodeIDFilter struct {
	nodeID uint32
}

func (f allowNodeIDFilter) Allow(_ balancerConfig.Info, e endpoint.Info) bool {
	return e.NodeID() == f.nodeID
}

func (f allowNodeIDFilter) String() string {
	return "allowNodeID"
}

func TestApplyDiscoveredEndpointsReleasesFilteredOutConns(t *testing.T) {
	ctx := context.Background()

	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	defer func() { _ = pool.RemoveRef(ctx) }()

	b := &Balancer{
		driverConfig: cfg,
		pool:         pool,
		balancerConfig: balancerConfig.Config{
			AllowFallback: false,
			Filter:        allowNodeIDFilter{nodeID: 1},
		},
	}

	e1 := endpoint.New("e1.example:2135", endpoint.WithID(1))
	e2 := endpoint.New("e2.example:2135", endpoint.WithID(2))

	b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{e1, e2}, "")
	require.Len(t, b.connections().All(), 2)
	require.Len(t, b.connections().prefer, 1)

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
		driverConfig:   config.New(),
		pool:           pool,
		balancerConfig: balancerConfig.Config{},
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		b.connectionsState.Store(newConnectionsState(
			[]conn.Conn{cc},
			nil,
			balancerConfig.Info{},
			true,
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		b.connectionsState.Store(newConnectionsState(nil,
			nil, balancerConfig.Info{}, true, nil,
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		b.connectionsState.Store(newConnectionsState(nil,
			nil, balancerConfig.Info{}, true, nil,
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
				driverConfig:   cfg,
				pool:           pool,
				balancerConfig: balancerConfig.Config{},
			}
			s := newConnectionsState([]conn.Conn{cc1}, nil, balancerConfig.Info{}, false, nil)
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false, nil)
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1}, nil, balancerConfig.Info{}, false, nil)
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false, nil)
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1}, nil, balancerConfig.Info{}, false, nil)
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false, nil)
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false, nil)
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false, nil)
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false, nil)
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
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false, nil)
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
