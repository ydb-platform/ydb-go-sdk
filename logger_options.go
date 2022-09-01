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

func WithOutWriter(out io.Writer) LoggerOption {
	return LoggerOption(logger.WithOutWriter(out))
}

func WithErrWriter(err io.Writer) LoggerOption {
	return LoggerOption(logger.WithErrWriter(err))
}
