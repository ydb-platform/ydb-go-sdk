package dial

import (
	"time"

	"google.golang.org/grpc/keepalive"
)

var (
	// DefaultKeepaliveInterval contains default duration between grpc keepalive
	DefaultKeepaliveInterval = 10 * time.Second
	MinKeepaliveInterval     = 10 * time.Second
	DefaultGRPCMsgSize       = 64 * 1024 * 1024 // 64MB
)

var (
	DefaultGrpcConnectionPolicy = keepalive.ClientParameters{
		Time:                DefaultKeepaliveInterval,
		Timeout:             MinKeepaliveInterval,
		PermitWithoutStream: true,
	}
)
