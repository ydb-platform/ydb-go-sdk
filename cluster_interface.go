package ydb

import (
	"google.golang.org/grpc"
)

type ClientConnInterface interface {
	grpc.ClientConnInterface
	Address() string
}

// Cluster interface provide main usage of YDB driver with
// code-generated grpc-clients
type Cluster interface {
	// ClientConnInterface interface allows Cluster use as grpc.ClientConnInterface
	// with lazy getting raw grpc-connection in Invoke() or NewStream() stages.
	// Lazy getting grpc-connection must use for embedded client-side balancing
	// Cluster may be put into code-generated client constructor as is.
	grpc.ClientConnInterface

	// Stats provide getting connections stats
	Stats(it func(Endpoint, ConnStats))

	// Close clears resources and close all connections to YDB
	Close() error
}
