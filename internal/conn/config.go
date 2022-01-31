package conn

import (
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config interface {
	DialTimeout() time.Duration
	RequestTimeout() time.Duration
	OperationTimeout() time.Duration
	OperationCancelAfter() time.Duration
	StreamTimeout() time.Duration
	Trace() ydb_trace.Driver
	ConnectionTTL() time.Duration
	GrpcDialOptions() []grpc.DialOption
}
