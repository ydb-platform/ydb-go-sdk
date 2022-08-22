package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"time"

	"google.golang.org/grpc"
	grpcCredentials "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xnet"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xresolver"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	// DefaultKeepaliveInterval contains default duration between grpc keepalive
	DefaultKeepaliveInterval    = 10 * time.Second
	MinKeepaliveInterval        = 10 * time.Second
	DefaultDialTimeout          = 5 * time.Second
	DefaultGRPCMsgSize          = 64 * 1024 * 1024 // 64MB
	DefaultGrpcConnectionPolicy = keepalive.ClientParameters{
		Time:                DefaultKeepaliveInterval,
		Timeout:             MinKeepaliveInterval,
		PermitWithoutStream: true,
	}
)

func grpcOptions(t trace.Driver, secure bool, tlsConfig *tls.Config) (opts []grpc.DialOption) {
	opts = append(opts,
		grpc.WithContextDialer(
			func(ctx context.Context, address string) (net.Conn, error) {
				return xnet.New(ctx, address, t)
			},
		),
		grpc.WithKeepaliveParams(
			DefaultGrpcConnectionPolicy,
		),
		grpc.WithDefaultServiceConfig(`{
				"loadBalancingPolicy": "round_robin"
			}`),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(DefaultGRPCMsgSize),
			grpc.MaxCallSendMsgSize(DefaultGRPCMsgSize),
		),
		grpc.WithResolvers(
			xresolver.New("", t),
			xresolver.New("ydb", t),
			xresolver.New("grpc", t),
			xresolver.New("grpcs", t),
		),
	)
	if secure {
		opts = append(opts, grpc.WithTransportCredentials(
			grpcCredentials.NewTLS(tlsConfig),
		))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		))
	}
	return opts
}

func certPool() *x509.CertPool {
	certPool, err := x509.SystemCertPool()
	if err == nil {
		return certPool
	}
	return x509.NewCertPool()
}

func defaultTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    certPool(),
	}
}

func defaultConfig() (c Config) {
	return Config{
		credentials: credentials.NewAnonymousCredentials(
			credentials.WithSourceInfo("default"),
		),
		balancerConfig: balancers.Default(),
		tlsConfig:      defaultTLSConfig(),
		dialTimeout:    DefaultDialTimeout,
	}
}
