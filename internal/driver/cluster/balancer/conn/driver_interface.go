package conn

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/addr"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"google.golang.org/grpc/metadata"
	"time"
)

type Driver interface {
	Cluster

	RequestTimeout() time.Duration
	OperationTimeout() time.Duration
	OperationCancelAfter() time.Duration
	Meta(ctx context.Context) (metadata.MD, error)
	Trace(ctx context.Context) trace.DriverTrace
	Pessimize(addr addr.Addr) error
	StreamTimeout() time.Duration
}
