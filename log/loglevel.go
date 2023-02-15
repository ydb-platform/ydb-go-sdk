package log

func logDebugWarn(logger Logger, err error, format string, args ...interface{}) {
	logLevel(logger, err, DEBUG, WARN, format, args...)
}

func logDebugInfo(logger Logger, err error, format string, args ...interface{}) {
	logLevel(logger, err, DEBUG, INFO, format, args...)
}

func logInfoWarn(logger Logger, err error, format string, args ...interface{}) {
	logLevel(logger, err, INFO, WARN, format, args...)
}

func logLevel(logger Logger, err error, okLevel, errLevel Level, format string, args ...interface{}) {
	level := okLevel
	if err != nil {
		level = errLevel

		format += "with error: %+v"
		args = append(args[:len(args):len(args)], err)
	}

	var logFunc func(format string, args ...interface{})
	switch level {
	case TRACE:
		logFunc = logger.Tracef
	case DEBUG:
		logFunc = logger.Debugf
	case INFO:
		logFunc = logger.Infof
	case WARN:
		logFunc = logger.Warnf
	case ERROR:
		logFunc = logger.Errorf
	case FATAL:
		logFunc = logger.Fatalf
	case QUIET:
		logFunc = func(format string, args ...interface{}) {
			// no logging
		}
	default:
		logFunc = logger.Errorf
		format = "unknown log level (%v): " + format
		args = append([]interface{}{level}, args...)
	}

	logFunc(format, args...)
}
