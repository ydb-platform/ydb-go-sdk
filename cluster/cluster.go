package cluster

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	"google.golang.org/grpc"
)

type ClientConnInterface interface {
	grpc.ClientConnInterface

	Addr() Addr
}

type DB interface {
	// ClientConnInterface interface allows DB use as grpc.ClientConnInterface
	// with lazy getting raw grpc-connection in Invoke() or NewStream() stages.
	// Lazy getting grpc-connection must use for embedded client-side balancing
	// DB may be put into code-generated client constructor as is.
	grpc.ClientConnInterface

	// Name returns database name
	Name() string
}

// Cluster interface provide main usage of YDB driver with
// code-generated grpc-clients
type Cluster interface {
	DB

	// Stats provide getting connections stats
	Stats(it func(Endpoint, stats.Stats))

	// Close clears resources and close all connections to YDB
	Close() error
}
