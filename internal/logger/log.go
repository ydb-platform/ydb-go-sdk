package logger

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

type Level int

const (
	TRACE = Level(iota)
	DEBUG
	INFO
	WARN
	ERROR
	FATAL

	QUIET

	trace = "TRACE"
	debug = "DEBUG"
	info  = "INFO"
	warn  = "WARN"
	error = "ERROR"
	fatal = "FATAL"
	quiet = "QUIET"
)

func (l Level) String() string {
	switch l {
	case TRACE:
		return trace
	case DEBUG:
		return debug
	case INFO:
		return info
	case WARN:
		return warn
	case ERROR:
		return error
	case FATAL:
		return fatal
	default:
		return quiet
	}
}

func FromString(l string) Level {
	switch strings.ToUpper(l) {
	case trace:
		return TRACE
	case debug:
		return DEBUG
	case info:
		return INFO
	case warn:
		return WARN
	case error:
		return ERROR
	case fatal:
		return FATAL
	default:
		return QUIET
	}
}

type logger struct {
	namespace string
	minLevel  Level
}

const dateLayout = "2006-01-02 15:04:05.000"

func New(namespace string, minLevel Level) *logger {
	return &logger{
		namespace: namespace,
		minLevel:  minLevel,
	}
}

func (l *logger) format(format string, logLevel Level) string {
	return fmt.Sprintf("%-5s %23s %-26s %s\n", logLevel.String(), time.Now().Format(dateLayout), "["+l.namespace+"]", format)
}

func (l *logger) Tracef(format string, args ...interface{}) {
	if l.minLevel > TRACE {
		return
	}
	fmt.Fprintf(os.Stdout, l.format(format, TRACE), args...)
}

func (l *logger) Debugf(format string, args ...interface{}) {
	if l.minLevel > DEBUG {
		return
	}
	fmt.Fprintf(os.Stdout, l.format(format, DEBUG), args...)
}

func (l *logger) Infof(format string, args ...interface{}) {
	if l.minLevel > INFO {
		return
	}
	fmt.Fprintf(os.Stdout, l.format(format, INFO), args...)
}

func (l *logger) Warnf(format string, args ...interface{}) {
	if l.minLevel > WARN {
		return
	}
	fmt.Fprintf(os.Stderr, l.format(format, WARN), args...)
}

func (l *logger) Errorf(format string, args ...interface{}) {
	if l.minLevel > ERROR {
		return
	}
	fmt.Fprintf(os.Stderr, l.format(format, ERROR), args...)
}

func (l *logger) Fatalf(format string, args ...interface{}) {
	if l.minLevel > FATAL {
		return
	}
	fmt.Fprintf(os.Stderr, l.format(format, FATAL), args...)
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	return a + "." + b
}

func (l *logger) WithName(name string) log.Logger {
	return &logger{
		namespace: join(l.namespace, name),
		minLevel:  l.minLevel,
	}
}
