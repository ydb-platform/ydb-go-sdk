package log

import (
	"io"
)

type Option func(l *logger)

func Nop() Option {
	return func(l *logger) {}
}

func WithColoring() Option {
	return func(l *logger) {
		l.coloring = true
	}
}

func WithMinLevel(level Level) Option {
	return func(l *logger) {
		l.minLevel = level
	}
}

func WithNamespace(namespace string) Option {
	return func(l *logger) {
		l.namespace = append(l.namespace, namespace)
	}
}

func WithWriter(w io.Writer) Option {
	return func(l *logger) {
		l.w = w
	}
}

type logOptions struct {
	logQuery bool
}

type option func(o *logOptions)

func WithLogQuery() option {
	return func(o *logOptions) {
		o.logQuery = true
	}
}

func parseOptions(opts ...option) logOptions {
	options := logOptions{}
	for _, o := range opts {
		if o != nil {
			o(&options)
		}
	}
	return options
}
