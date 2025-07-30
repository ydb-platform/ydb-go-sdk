package balancer

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/test/bufconn"
)

func TestBalancer_discoveryConn(t *testing.T) {
	fakeListener := bufconn.Listen(1024 * 1024)
	fakeServer := grpc.NewServer()
	defer fakeServer.Stop()

	go fakeServer.Serve(fakeListener)

	dialAttempt := 0

	balancer := &Balancer{
		address: "ydbmock:///mock",
		driverConfig: config.New(
			config.WithEndpoint("mock"),
			config.WithGrpcOptions(
				grpc.WithResolvers(&mockResolverBuilder{}),

				grpc.WithContextDialer(
					// The first dialing is very long and ends with an error, while the subsequent ones work fine.
					func(ctx context.Context, s string) (net.Conn, error) {
						dialAttempt++

						if dialAttempt == 1 {
							time.Sleep(1 * time.Hour) //extremely slow dialing

							return nil, fmt.Errorf("fake error for endpoint: %s", s)
						}

						return fakeListener.DialContext(ctx)
					}),

				// If you want to reproduce the issue, uncomment the line:
				// grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy": "pick_first"}`),
			),
		),
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	_, err := balancer.discoveryConn(ctx)
	require.NoError(t, err)
}

// Mock resolver
//

type mockResolverBuilder struct{}

func (r *mockResolverBuilder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (
	resolver.Resolver, error) {
	state := resolver.State{Addresses: []resolver.Address{
		{Addr: "mockaddress1"},
		{Addr: "mockaddress2"},
	}}
	cc.UpdateState(state)
	return &mockResover{}, nil
}

func (r *mockResolverBuilder) Scheme() string { return "ydbmock" }

type mockResover struct{}

func (r *mockResover) ResolveNow(resolver.ResolveNowOptions) {}
func (r *mockResover) Close()                                {}
