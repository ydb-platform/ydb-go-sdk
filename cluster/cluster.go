package cluster

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/endpoint"
)

type ClientConnInterface interface {
	grpc.ClientConnInterface

	Endpoint() endpoint.Endpoint
}

type Cluster interface {
	// ClientConnInterface interface allows DB use as grpc.ClientConnInterface
	// with lazy getting raw grpc-connection in Invoke() or NewStream() stages.
	// Lazy getting grpc-connection must use for embedded client-side balancing
	// DB may be put into code-generated client constructor as is.
	grpc.ClientConnInterface

	// Close clears resources and close all connections to YDB
	Close(ctx context.Context) error
}
