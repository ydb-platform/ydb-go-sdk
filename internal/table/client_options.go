package table

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"

// NewClientOption configures table client construction.
type NewClientOption func(*newClientOptions)

type newClientOptions struct {
	sharedSessionPool *query.SharedSessionPool
}

// WithSharedSessionPool leases sessions from the driver-level pool.
func WithSharedSessionPool(pool *query.SharedSessionPool) NewClientOption {
	return func(o *newClientOptions) {
		o.sharedSessionPool = pool
	}
}
