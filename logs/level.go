package logs

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger/level"
)

type Level level.Level

const (
	TRACE = Level(level.TRACE)
	DEBUG = Level(level.DEBUG)
	INFO  = Level(level.INFO)
	WARN  = Level(level.WARN)
	ERROR = Level(level.ERROR)
	FATAL = Level(level.FATAL)

	QUIET = Level(level.QUIET)
)

func FromString(l string) Level {
	return Level(level.FromString(l))
}
