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

func withExternalLogger(ll Logger) Option {
	return func(l *logger) {
		l.externalLogger = ll
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

func WithLogQuery() Option {
	return func(l *logger) {
		l.logQuery = true
	}
}

func WithWriter(w io.Writer) Option {
	return func(l *logger) {
		l.w = w
	}
}
