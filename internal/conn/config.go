package conn

import (
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config interface {
	Trace() *trace.Driver
	GrpcDialOptions() []grpc.DialOption
}
