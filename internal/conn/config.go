package conn

import (
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config interface {
	DialTimeout() time.Duration
	ConnectionTTL() time.Duration
	Trace() *trace.Driver
	GrpcDialOptions() []grpc.DialOption
	// AddressFilter returns an optional function that is called for every
	// resolved IP:port address before it is forwarded to gRPC.  Addresses for
	// which the function returns false are dropped.  A nil return value means
	// no filtering.
	AddressFilter() func(addr string) bool
}
