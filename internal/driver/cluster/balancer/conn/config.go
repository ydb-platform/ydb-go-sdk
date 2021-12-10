package conn

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config interface {
	DialTimeout() time.Duration
	RequestTimeout() time.Duration
	OperationTimeout() time.Duration
	OperationCancelAfter() time.Duration
	StreamTimeout() time.Duration
	Meta(ctx context.Context) (context.Context, error)
	Trace(ctx context.Context) trace.Driver
	Pessimize(ctx context.Context, endpoint endpoint.Endpoint) error
}
