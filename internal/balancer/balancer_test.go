package balancer

import (
	"context"
	"fmt"
	"net"
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
)

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
	defer func() { _ = pool.Release(ctx) }()

	b := &Balancer{
		driverConfig:   cfg,
		pool:           pool,
		balancerConfig: balancerConfig.Config{},
	}

	initial := newConnectionsState(nil, b.balancerConfig.Filter, balancerConfig.Info{}, b.balancerConfig.AllowFallback)
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
	require.Equal(t, e1.Address(), all[0].Address())
	require.Equal(t, e1.NodeID(), all[0].NodeID())
	require.Equal(t, e2.Address(), all[1].Address())
	require.Equal(t, e2.NodeID(), all[1].NodeID())

	// partially replace endpoints
	e3 := endpoint.New("e3.example:2135", endpoint.WithIPV6([]string{"2001:db8::3"}), endpoint.WithID(1))
	b.applyDiscoveredEndpoints(ctx, []endpoint.Endpoint{e2, e3}, "")
	// connectionsState should be updated and reflect the endpoints
	after = b.connections()
	require.NotNil(t, after)
	all = after.All()
	require.Equal(t, 2, len(all))
	require.Equal(t, e2.Address(), all[0].Address())
	require.Equal(t, e2.NodeID(), all[0].NodeID())
	require.Equal(t, e3.Address(), all[1].Address())
	require.Equal(t, e3.NodeID(), all[1].NodeID())
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
			AddrField:           "node1:2135", NodeIDField: 1, State: state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.Release(ctx) }()

		b := &Balancer{
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1}, nil, balancerConfig.Info{}, false)
		b.connectionsState.Store(s)

		err := b.Invoke(ctx, "/test.Service/Method", nil, nil)
		require.NoError(t, err)
		require.NotEqual(t, state.Banned, cc1.GetState())
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
			State:               state.Online,
		}
		cc2 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node2:2135",
			NodeIDField:         2,
			State:               state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.Release(ctx) }()

		b := &Balancer{
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false)
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
		require.Equal(t, state.Banned, cc1.GetState())

		// nextConn must only return cc2 now.
		for i := 0; i < 100; i++ {
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
			AddrField:           "node1:2135", NodeIDField: 1, State: state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.Release(ctx) }()

		b := &Balancer{
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1}, nil, balancerConfig.Info{}, false)
		b.connectionsState.Store(s)

		// Context only bans on OVERLOADED — a NOT_FOUND error must not ban.
		invokeCtx := BanOnOperationError(ctx, Ydb.StatusIds_OVERLOADED)
		err := b.Invoke(invokeCtx, "/test.Service/Method", nil, nil)
		require.Error(t, err)
		require.NotEqual(t, state.Banned, cc1.GetState())
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
			AddrField:           "node1:2135", NodeIDField: 1, State: state.Online,
		}
		cc2 := &mock.Conn{
			ClientConnInterface: cc,
			AddrField:           "node2:2135", NodeIDField: 2, State: state.Online,
		}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.Release(ctx) }()

		b := &Balancer{
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false)
		b.connectionsState.Store(s)

		// Sequentially pessimize cc1 then cc2 via the normal Invoke+BanOnOperationError flow.
		cc1Ctx := BanOnOperationError(endpoint.WithNodeID(ctx, cc1.NodeIDField), Ydb.StatusIds_OVERLOADED)
		err := b.Invoke(cc1Ctx, "/test.Service/Method", nil, nil)
		require.Error(t, err)
		require.Equal(t, state.Banned, cc1.GetState())

		cc2Ctx := BanOnOperationError(endpoint.WithNodeID(ctx, cc2.NodeIDField), Ydb.StatusIds_OVERLOADED)
		err = b.Invoke(cc2Ctx, "/test.Service/Method", nil, nil)
		require.Error(t, err)
		require.Equal(t, state.Banned, cc2.GetState())

		// When all connections are banned, the balancer must still return a connection
		// (falling back to the banned connections pool so callers can retry).
		c, err := b.nextConn(ctx)
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, state.Banned, c.GetState())
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
			AddrField:           "node1:2135", NodeIDField: 1, State: state.Online,
		}
		cc2 := &mock.Conn{AddrField: "node2:2135", NodeIDField: 2, State: state.Online}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.Release(ctx) }()

		b := &Balancer{
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false)
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
		require.Equal(t, state.Banned, cc1.GetState())

		for i := 0; i < 10; i++ {
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
			AddrField:           "node1:2135", NodeIDField: 1, State: state.Online,
		}
		cc2 := &mock.Conn{AddrField: "node2:2135", NodeIDField: 2, State: state.Online}

		cfg := config.New()
		pool := conn.NewPool(ctx, cfg)
		defer func() { _ = pool.Release(ctx) }()

		b := &Balancer{
			driverConfig:   cfg,
			pool:           pool,
			balancerConfig: balancerConfig.Config{},
		}
		s := newConnectionsState([]conn.Conn{cc1, cc2}, nil, balancerConfig.Info{}, false)
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
		require.Equal(t, state.Banned, cc1.GetState())

		for i := 0; i < 10; i++ {
			c, nextErr := b.nextConn(ctx)
			require.NoError(t, nextErr)
			require.Equal(t, cc2.AddrField, c.Endpoint().Address())
		}
	})
}
