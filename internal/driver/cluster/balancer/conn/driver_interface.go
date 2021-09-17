package conn

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"google.golang.org/grpc/metadata"
	"time"
)

type Driver interface {
	cluster.Cluster

	RequestTimeout() time.Duration
	OperationTimeout() time.Duration
	OperationCancelAfter() time.Duration
	Meta(ctx context.Context) (metadata.MD, error)
	Trace(ctx context.Context) trace.DriverTrace
	Pessimize(addr cluster.Addr) error
	StreamTimeout() time.Duration
}
