package traces

import "github.com/ydb-platform/ydb-go-sdk/v3/logs"

// logger is a sugared wrapper over logs.Logger
type logger struct {
	l     logs.Logger
	scope []string
}

func (l logger) Log(lvl logs.Level, msg string, fields ...logs.Field) {
	l.l.Log(logs.Options{
		Lvl:   lvl,
		Scope: l.scope,
	}, msg, fields...)
}

func (l logger) Trace(msg string, fields ...logs.Field) {
	l.Log(logs.TRACE, msg, fields...)
}

func (l logger) Debug(msg string, fields ...logs.Field) {
	l.Log(logs.DEBUG, msg, fields...)
}

func (l logger) Info(msg string, fields ...logs.Field) {
	l.Log(logs.INFO, msg, fields...)
}

func (l logger) Warn(msg string, fields ...logs.Field) {
	l.Log(logs.WARN, msg, fields...)
}

func (l logger) Error(msg string, fields ...logs.Field) {
	l.Log(logs.ERROR, msg, fields...)
}

func (l logger) Fatal(msg string, fields ...logs.Field) {
	l.Log(logs.FATAL, msg, fields...)
}
