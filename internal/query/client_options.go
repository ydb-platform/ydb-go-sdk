package query

// NewClientOption configures query client construction.
type NewClientOption func(*newClientOptions)

type newClientOptions struct {
	sessionPool *SessionPool
}

// WithSessionPool leases explicit sessions from the driver-level pool.
func WithSessionPool(pool *SessionPool) NewClientOption {
	return func(o *newClientOptions) {
		o.sessionPool = pool
	}
}
