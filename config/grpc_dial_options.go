package config

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/resolver"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func (c *config) GrpcDialOptions() (opts []grpc.DialOption) {
	// nolint:gocritic
	opts = append(
		c.grpcOptions,
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			return newConn(ctx, address, trace.ContextDriver(ctx).Compose(c.trace))
		}),
		grpc.WithKeepaliveParams(DefaultGrpcConnectionPolicy),
		grpc.WithResolvers(
			resolver.New("ydb"),
		),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy": "round_robin"}`),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(DefaultGRPCMsgSize),
			grpc.MaxCallSendMsgSize(DefaultGRPCMsgSize),
		),
		grpc.WithBlock(),
	)
	if c.secure {
		opts = append(opts, grpc.WithTransportCredentials(
			credentials.NewTLS(c.tlsConfig),
		))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return
}
