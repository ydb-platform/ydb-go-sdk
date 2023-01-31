package log

import "github.com/ydb-platform/ydb-go-sdk/v3/logs"

type Level logs.Level

const (
	TRACE = Level(logs.TRACE)
	DEBUG = Level(logs.DEBUG)
	INFO  = Level(logs.INFO)
	WARN  = Level(logs.WARN)
	ERROR = Level(logs.ERROR)
	FATAL = Level(logs.FATAL)

	QUIET = Level(logs.QUIET)
)

func FromString(l string) Level {
	return Level(logs.FromString(l))
}
