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
