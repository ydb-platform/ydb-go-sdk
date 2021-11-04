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
	info  = " INFO"
	warn  = " WARN"
	error = "ERROR"
	fatal = "FATAL"
	quiet = "QUIET"

	colouredTrace = "\033[47m" + trace
	colouredDebug = "\033[100m" + debug
	colouredInfo  = "\033[106m" + info
	colouredWarn  = "\033[103m" + warn
	colouredError = "\033[101m" + error
	colouredFatal = "\033[101m" + fatal
	colouredQuiet = quiet
)

//func init() {
//	for i := 0; i < 256; i++ {
//		fmt.Printf("\u001B[%dm color %d\u001B[0m\n", i, i)
//	}
//	os.Exit(0)
//}
//
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

func (l Level) Colored() string {
	switch l {
	case TRACE:
		return colouredTrace
	case DEBUG:
		return colouredDebug
	case INFO:
		return colouredInfo
	case WARN:
		return colouredWarn
	case ERROR:
		return colouredError
	case FATAL:
		return colouredFatal
	default:
		return colouredQuiet
	}
}

func FromString(l string) Level {
	switch strings.ToUpper(l) {
	case strings.TrimSpace(trace):
		return TRACE
	case strings.TrimSpace(debug):
		return DEBUG
	case strings.TrimSpace(info):
		return INFO
	case strings.TrimSpace(warn):
		return WARN
	case strings.TrimSpace(error):
		return ERROR
	case strings.TrimSpace(fatal):
		return FATAL
	default:
		return QUIET
	}
}

type logger struct {
	namespace string
	minLevel  Level
}

const (
	dateLayout = "2006-01-02 15:04:05.000"

	colorReset = "\033[0m"

	colorTrace = "\033[38m"
	colorDebug = "\033[37m"
	colorInfo  = "\033[36m"
	colorWarn  = "\033[33m"
	colorError = "\033[31m"
	colorFatal = "\033[41m"
	colorQuiet = colorReset
)

func color(l Level) string {
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

func New(namespace string, minLevel Level) *logger {
	return &logger{
		namespace: namespace,
		minLevel:  minLevel,
	}
}

func (l *logger) format(format string, logLevel Level) string {
	return fmt.Sprintf("%5s %23s %26s%s %s%s%s\n", logLevel.Colored(), time.Now().Format(dateLayout), "["+l.namespace+"]", colorReset, color(logLevel), format, colorReset)
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
