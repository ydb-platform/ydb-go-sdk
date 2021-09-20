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
	ConnectionTLL() time.Duration
}
