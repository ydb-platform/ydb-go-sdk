package conn

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"time"
)

type Config interface {
	RequestTimeout() time.Duration
	OperationTimeout() time.Duration
	OperationCancelAfter() time.Duration
	Meta(ctx context.Context) (context.Context, error)
	Trace(ctx context.Context) trace.DriverTrace
	Pessimize(addr cluster.Addr) error
	StreamTimeout() time.Duration
	KeepalivePolicy() *KeepalivePolicy
}

type KeepalivePolicy struct {
	// LazyConnect define moment for connect to endpoint
	// If LazyConnect is true connect to endpoint will start after getting endpoints list from discovery
	// If LazyConnect is false connect to endpoint will start during on RPC
	// By default LazyConnect is true
	LazyConnect bool

	// Timeout is a unified setting for control keep-alive or close of idle grpc connections
	// If Timeout equal zero Timeout will be set to dial.DefaultKeepaliveInterval
	// If Timeout less zero Timeout will be use as time-to-live value for closing idle grpc connection
	// If Timeout great zero Timeout will be use as keep-alive interval
	// By default Timeout is sets to dial.DefaultKeepaliveInterval
	Timeout time.Duration
}
