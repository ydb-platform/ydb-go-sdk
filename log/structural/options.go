package structural

type logOptions struct {
	logQuery bool
}

// Option is exported only for compatibility reasons and MUST NOT be initialized directly.
// Instead, only options providing functions (e.g. WithLogQuery()) must be used.
type Option func(o *logOptions)

func WithLogQuery() Option {
	return func(o *logOptions) {
		o.logQuery = true
	}
}

func parseOptions(opts ...Option) logOptions {
	options := logOptions{}
	for _, o := range opts {
		o(&options)
	}
	return options
}
