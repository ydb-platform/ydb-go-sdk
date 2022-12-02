package logs

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

type Level log.Level

const (
	TRACE = Level(log.TRACE)
	DEBUG = Level(log.DEBUG)
	INFO  = Level(log.INFO)
	WARN  = Level(log.WARN)
	ERROR = Level(log.ERROR)
	FATAL = Level(log.FATAL)

	QUIET = Level(log.QUIET)
)
