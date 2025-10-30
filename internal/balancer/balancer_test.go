package balancer

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
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
