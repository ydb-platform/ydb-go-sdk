package query

// NewClientOption configures query client construction.
type NewClientOption func(*newClientOptions)

type newClientOptions struct {
	sharedSessionPool *SharedSessionPool
}

// WithSharedSessionPool leases explicit sessions from the driver-level pool.
func WithSharedSessionPool(pool *SharedSessionPool) NewClientOption {
	return func(o *newClientOptions) {
		o.sharedSessionPool = pool
	}
}
