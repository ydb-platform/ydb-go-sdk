package config

import (
	"time"

	"google.golang.org/grpc/keepalive"
)

var (
	// DefaultKeepaliveInterval contains default duration between grpc keepalive
	DefaultKeepaliveInterval = 10 * time.Second
	MinKeepaliveInterval     = 10 * time.Second
	DefaultDialTimeout       = 5 * time.Second
	//nolint:lll
	DefaultGRPCMsgSize          = 64000000 // 64 million bytes, same as server default: https://github.com/ydb-platform/ydb/blob/19a39b96b1a403d2ab230d1f4e1c9324cb71e047/ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/grpc_common/constants.h#L7
	DefaultGrpcConnectionPolicy = keepalive.ClientParameters{
		Time:                DefaultKeepaliveInterval,
		Timeout:             MinKeepaliveInterval,
		PermitWithoutStream: true,
	}
)
