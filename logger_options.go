package ydb

import (
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

// LoggerOption
//
// Deprecated: use package log.Option directly
type LoggerOption = log.Option

// WithNamespace
//
// Deprecated: use package log.WithNamespace directly
func WithNamespace(namespace string) LoggerOption {
	return log.WithNamespace(namespace)
}

// WithMinLevel
//
// Deprecated: use package log.WithMinLevel directly
func WithMinLevel(minLevel log.Level) LoggerOption {
	return log.WithMinLevel(minLevel)
}

// WithNoColor specified coloring of log messages
//
// Deprecated: has no effect now, use WithColoring instead
func WithNoColor(b bool) LoggerOption {
	return log.Nop()
}

// WithColoring
//
// Deprecated: use package log.WithColoring directly
func WithColoring() LoggerOption {
	return log.WithColoring()
}

// WithOutWriter specified writer stream for internal logger
//
// Deprecated: use WithWriter instead
func WithOutWriter(w io.Writer) LoggerOption {
	return log.WithWriter(w)
}

// WithErrWriter specified writer stream for internal logger error messages
//
// Deprecated: use WithWriter instead
func WithErrWriter(io.Writer) LoggerOption {
	return log.Nop()
}

// WithWriter specified writer stream for internal logger
//
// Deprecated: use package log.WithWriter directly
func WithWriter(w io.Writer) LoggerOption {
	return log.WithWriter(w)
}
