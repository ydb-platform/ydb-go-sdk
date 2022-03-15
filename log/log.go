package log

type Logger interface {
	// Tracef logs at Trace logger level using fmt formatter
	Tracef(format string, args ...interface{})
	// Debugf logs at Debug logger level using fmt formatter
	Debugf(format string, args ...interface{})
	// Infof logs at Info logger level using fmt formatter
	Infof(format string, args ...interface{})
	// Warnf logs at Warn logger level using fmt formatter
	Warnf(format string, args ...interface{})
	// Errorf logs at Error logger level using fmt formatter
	Errorf(format string, args ...interface{})
	// Fatalf logs at Fatal logger level using fmt formatter
	Fatalf(format string, args ...interface{})

	// WithName provide applying sub-scope of logger messages
	WithName(name string) Logger
}

func logf(l Logger, level Level, format string, args ...interface{}) {
	switch level {
	case TRACE:
		l.Tracef(format, args...)
	case DEBUG:
		l.Debugf(format, args...)
	case INFO:
		l.Infof(format, args...)
	case WARN:
		l.Warnf(format, args...)
	case ERROR:
		l.Errorf(format, args...)
	case FATAL:
		l.Fatalf(format, args...)
	}
}
