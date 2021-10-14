package cluster

import (
	"context"

	"google.golang.org/grpc"
)

type ClientConnInterface interface {
	grpc.ClientConnInterface

	Address() string
}

type Cluster interface {
	// ClientConnInterface interface allows DB use as grpc.ClientConnInterface
	// with lazy getting raw grpc-connection in Invoke() or NewStream() stages.
	// Lazy getting grpc-connection must use for embedded client-side balancing
	// DB may be put into code-generated client constructor as is.
	grpc.ClientConnInterface

	// Name returns database name
	Name() string

	// Secure returns true if database connection is secure
	Secure() bool

	// Close clears resources and close all connections to YDB
	Close(ctx context.Context) error
}
