package router

import (
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
)

type Router interface {
	// ClientConnInterface interface allows Router use as grpc.ClientConnInterface
	// with lazy getting raw grpc-connection in Invoke() or NewStream() stages.
	// Lazy getting grpc-connection must use for embedded into driver client-side
	// balancing may be put into code-generated client constructor as is.
	grpc.ClientConnInterface
	closer.Closer
}

type Discoverer interface {
	Discovery() discovery.Client
}

type Info interface {
	// Endpoint returns initial endpoint
	Endpoint() string

	// Name returns router name
	Name() string

	// Secure returns true if router connection is secure
	Secure() bool
}

type Connection interface {
	Info
	Router
	Discoverer
}
