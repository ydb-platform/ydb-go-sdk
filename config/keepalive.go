package config

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"google.golang.org/grpc/keepalive"
	"time"
)

type GrpcConnectionPolicy conn.GrpcConnectionPolicy

var (
	// DefaultKeepaliveInterval contains default duration between grpc keepalive
	DefaultKeepaliveInterval = 5 * time.Minute
	MinKeepaliveInterval     = 1 * time.Minute
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
		TTL: 0,
	}
)
