package conn

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"google.golang.org/grpc/keepalive"
	"time"
)

type Config interface {
	RequestTimeout() time.Duration
	OperationTimeout() time.Duration
	OperationCancelAfter() time.Duration
	Meta(ctx context.Context) (context.Context, error)
	Trace(ctx context.Context) trace.Driver
	Pessimize(addr endpoint.Addr) error
	StreamTimeout() time.Duration
	GrpcConnectionPolicy() *GrpcConnectionPolicy
}

type GrpcConnectionPolicy struct {
	keepalive.ClientParameters

	// TTL is a duration for automatically close idle connections
	// Zero TTL will disable automatically closing of idle connections
	// By default TTL is sets to dial.DefaultGrpcConnectionTTL
	TTL time.Duration
}
