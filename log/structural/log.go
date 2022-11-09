package structural

import "time"

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

	Error(value error) Record

	Message(msg string)
}
