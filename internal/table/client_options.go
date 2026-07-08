package table

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"

// NewClientOption configures table client construction.
type NewClientOption func(*newClientOptions)

type newClientOptions struct {
	sessionPool *query.SessionPool
}

// WithSessionPool leases sessions from the driver-level pool.
func WithSessionPool(pool *query.SessionPool) NewClientOption {
	return func(o *newClientOptions) {
		o.sessionPool = pool
	}
}
