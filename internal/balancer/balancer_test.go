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
	"google.golang.org/grpc/test/bufconn"
)

func TestBalancer_discoveryConn(t *testing.T) {
	fakeListener := bufconn.Listen(1024 * 1024)
	fakeServer := grpc.NewServer()
	defer fakeServer.Stop()

	go fakeServer.Serve(fakeListener)

	dialAttempt := 0

	balancer := &Balancer{
		address: "ydb:///example.com:2135", // `example.com` has several IPs, TODO: replace with custom resolver
		driverConfig: config.New(
			config.WithGrpcOptions(
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
