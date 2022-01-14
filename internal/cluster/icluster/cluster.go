package icluster

import (
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
)

type Cluster interface {
	// ClientConnInterface interface allows Cluster use as grpc.ClientConnInterface
	// with lazy getting raw grpc-connection in Invoke() or NewStream() stages.
	// Lazy getting grpc-connection must use for embedded client-side balancing
	// DB may be put into code-generated client constructor as is.
	grpc.ClientConnInterface
	closer.Closer
}
