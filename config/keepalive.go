package config

import (
	"time"

	"google.golang.org/grpc/keepalive"
)

type GrpcConnectionPolicy struct {
	keepalive.ClientParameters

	// TTL is a duration for automatically close idle connections
	// Zero TTL will disable automatically closing of idle connections
	// By default TTL is sets to DefaultGrpcConnectionTTL
	TTL time.Duration
}

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
