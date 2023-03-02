package logger

import (
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger/level"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
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

func WithMinLevel(level level.Level) Option {
	return func(l *logger) {
		l.minLevel = level
	}
}

func WithNamespace(namespace string) Option {
	return func(l *logger) {
		l.namespace = namespace
	}
}

func WithExternalLogger(external log.Logger) Option {
	return func(l *logger) {
		l.external = external
	}
}

func WithWriter(w io.Writer) Option {
	return func(l *logger) {
		l.w = w
	}
}
