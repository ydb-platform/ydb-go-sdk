package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
)

type Level structural.Level

const (
	TRACE = Level(structural.TRACE)
	DEBUG = Level(structural.DEBUG)
	INFO  = Level(structural.INFO)
	WARN  = Level(structural.WARN)
	ERROR = Level(structural.ERROR)
	FATAL = Level(structural.FATAL)

	QUIET = Level(structural.QUIET)
)

func FromString(l string) Level {
	return Level(structural.FromString(l))
}
