package config

import (
	"time"

	"google.golang.org/grpc/keepalive"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
)

type GrpcConnectionPolicy conn.GrpcConnectionPolicy

var (
	// DefaultKeepaliveInterval contains default duration between grpc keepalive
	DefaultKeepaliveInterval = 10 * time.Second
	MinKeepaliveInterval     = 10 * time.Second
	DefaultGrpcConnectionTTL = 6 * time.Minute
	DefaultGRPCMsgSize       = 64 * 1024 * 1024 // 64MB
)

var (
	DefaultGrpcConnectionPolicy = GrpcConnectionPolicy{
		ClientParameters: keepalive.ClientParameters{
			Time:                DefaultKeepaliveInterval,
			Timeout:             MinKeepaliveInterval,
			PermitWithoutStream: true,
		},
		TTL: DefaultGrpcConnectionTTL,
	}
)
