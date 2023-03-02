package ydb

import (
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger/level"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

type LoggerOption logger.Option

func WithNamespace(namespace string) LoggerOption {
	return LoggerOption(logger.WithNamespace(namespace))
}

func WithMinLevel(minLevel log.Level) LoggerOption {
	return LoggerOption(logger.WithMinLevel(level.Level(minLevel)))
}

// WithNoColor specified coloring of log messages
//
// Deprecated: has no effect now, use WithColoring instead
func WithNoColor(b bool) LoggerOption {
	return LoggerOption(logger.Nop())
}

func WithColoring() LoggerOption {
	return LoggerOption(logger.WithColoring())
}

func WithExternalLogger(external log.Logger) LoggerOption {
	return LoggerOption(logger.WithExternalLogger(external))
}

// WithOutWriter specified writer stream for internal logger
//
// Deprecated: use WithWriter instead
func WithOutWriter(w io.Writer) LoggerOption {
	return LoggerOption(logger.WithWriter(w))
}

// WithErrWriter specified writer stream for internal logger error messages
//
// Deprecated: use WithWriter instead
func WithErrWriter(io.Writer) LoggerOption {
	return LoggerOption(logger.Nop())
}

// WithWriter specified writer stream for internal logger
func WithWriter(w io.Writer) LoggerOption {
	return LoggerOption(logger.WithWriter(w))
}
