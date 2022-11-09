package structural

import (
	"time"
)

type Logger interface {
	Trace() Record
	Debug() Record
	Info() Record
	Warn() Record
	Error() Record
	Fatal() Record

	WithName(name string) Logger
}

type Record interface {
	String(key string, value string) Record
	Strings(key string, value []string) Record

	Duration(key string, value time.Duration) Record

	Int(key string, value int) Record
	Int64(key string, value int64) Record

	Bool(key string, value bool) Record

	Error(value error) Record
	NamedError(key string, value error) Record

	Any(key string, value interface{}) Record

	Message(msg string)
}
