package cluster

import (
	"context"

	"google.golang.org/grpc"
)

type Cluster interface {
	// ClientConnInterface interface allows Cluster use as grpc.ClientConnInterface
	// with lazy getting raw grpc-connection in Invoke() or NewStream() stages.
	// Lazy getting grpc-connection must use for embedded client-side balancing
	// DB may be put into code-generated client constructor as is.
	grpc.ClientConnInterface

	// Close clears resources and close all connections to YDB
	Close(ctx context.Context) error
}
