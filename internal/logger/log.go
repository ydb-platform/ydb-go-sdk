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

	dateLayout = "2006-01-02 15:04:05.000"
)

const (
	colorReset = "\033[0m"

	colorTrace = "\033[38m"
	colorDebug = "\033[37m"
	colorInfo  = "\033[36m"
	colorWarn  = "\033[33m"
	colorError = "\033[31m"
	colorFatal = "\033[41m"
	colorQuiet = colorReset

	colorTraceBold = "\033[47m"
	colorDebugBold = "\033[100m"
	colorInfoBold  = "\033[106m"
	colorWarnBold  = "\033[103m"
	colorErrorBold = "\033[101m"
	colorFatalBold = "\033[101m"
	colorQuietBold = ""
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

func (l Level) boldColor() string {
	switch l {
	case TRACE:
		return colorTraceBold
	case DEBUG:
		return colorDebugBold
	case INFO:
		return colorInfoBold
	case WARN:
		return colorWarnBold
	case ERROR:
		return colorErrorBold
	case FATAL:
		return colorFatalBold
	default:
		return colorQuietBold
	}
}

func (l Level) color() string {
	switch l {
	case TRACE:
		return colorTrace
	case DEBUG:
		return colorDebug
	case INFO:
		return colorInfo
	case WARN:
		return colorWarn
	case ERROR:
		return colorError
	case FATAL:
		return colorFatal
	default:
		return colorQuiet
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
	noColor   bool
}

type option func(l *logger)

func WithNoColor(b bool) option {
	return func(l *logger) {
		l.noColor = b
	}
}

func WithMinLevel(level Level) option {
	return func(l *logger) {
		l.minLevel = level
	}
}

func WithNamespace(namespace string) option {
	return func(l *logger) {
		l.namespace = namespace
	}
}

func New(opts ...option) *logger {
	l := &logger{}
	for _, o := range opts {
		o(l)
	}
	return l
}

func (l *logger) format(format string, logLevel Level) string {
	if l.noColor {
		return fmt.Sprintf("%-5s %23s %26s %s\n", logLevel.String(), time.Now().Format(dateLayout), "["+l.namespace+"]", format)
	}
	return fmt.Sprintf("%s%-5s %23s %26s%s %s%s%s\n", logLevel.boldColor(), logLevel.String(), time.Now().Format(dateLayout), "["+l.namespace+"]", colorReset, logLevel.color(), format, colorReset)
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
		noColor:   l.noColor,
	}
}
