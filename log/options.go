package log

type logOptions struct {
	logQuery bool
}

type option func(o *logOptions)

func WIthLogQuery() option {
	return func(o *logOptions) {
		o.logQuery = true
	}
}

func parseOptions(opts ...option) logOptions {
	options := logOptions{}
	for _, o := range opts {
		o(&options)
	}
	return options
}
