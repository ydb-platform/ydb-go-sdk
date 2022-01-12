package db

import (
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
)

type Connection interface {
	grpc.ClientConnInterface
	closer.Closer

	// Endpoint returns initial endpoint
	Endpoint() string

	// Name returns database name
	Name() string

	// Secure returns true if database connection is secure
	Secure() bool
}
