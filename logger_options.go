package ydb

import (
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

type LoggerOption log.Option

func WithNamespace(namespace string) LoggerOption {
	return LoggerOption(log.WithNamespace(namespace))
}

func WithMinLevel(minLevel log.Level) LoggerOption {
	return LoggerOption(log.WithMinLevel(minLevel))
}

// WithNoColor specified coloring of log messages
//
// Deprecated: has no effect now, use WithColoring instead
func WithNoColor(b bool) LoggerOption {
	return LoggerOption(log.Nop())
}

func WithColoring() LoggerOption {
	return LoggerOption(log.WithColoring())
}

// WithOutWriter specified writer stream for internal logger
//
// Deprecated: use WithWriter instead
func WithOutWriter(w io.Writer) LoggerOption {
	return LoggerOption(log.WithWriter(w))
}

// WithErrWriter specified writer stream for internal logger error messages
//
// Deprecated: use WithWriter instead
func WithErrWriter(io.Writer) LoggerOption {
	return LoggerOption(log.Nop())
}

// WithWriter specified writer stream for internal logger
func WithWriter(w io.Writer) LoggerOption {
	return LoggerOption(log.WithWriter(w))
}
