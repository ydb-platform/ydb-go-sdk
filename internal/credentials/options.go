package credentials

type optionsHolder struct {
	sourceInfo string
}

type Option func(opts *optionsHolder)

func WithSourceInfo(sourceInfo string) Option {
	return func(opts *optionsHolder) {
		opts.sourceInfo = sourceInfo
	}
}
