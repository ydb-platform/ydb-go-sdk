package config

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"time"
)

type KeepalivePolicy conn.KeepalivePolicy

var (
	// DefaultKeepaliveInterval contains default duration between grpc keepalive
	DefaultKeepaliveInterval = 5 * time.Minute
	MinKeepaliveInterval     = 1 * time.Minute
	DefaultGRPCMsgSize       = 64 * 1024 * 1024 // 64MB
)

var (
	DefaultKeepalivePolicy = KeepalivePolicy{LazyConnect: false, Timeout: DefaultKeepaliveInterval}
)
