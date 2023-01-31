package traces

type LogOptions struct {
	LogQuery bool
}

type Option func(o *LogOptions)

func WithLogQuery() Option {
	return func(o *LogOptions) {
		o.LogQuery = true
	}
}

func ParseOptions(opts ...Option) LogOptions {
	options := LogOptions{}
	for _, o := range opts {
		o(&options)
	}
	return options
}
