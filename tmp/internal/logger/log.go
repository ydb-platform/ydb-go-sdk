package logger

import (
	"fmt"
	"io"
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

	lblTrace = "TRACE"
	lblDebug = "DEBUG"
	lblInfo  = "INFO"
	lblWarn  = "WARN"
	lblError = "ERROR"
	lblFatal = "FATAL"
	lblQuiet = "QUIET"

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
		return lblTrace
	case DEBUG:
		return lblDebug
	case INFO:
		return lblInfo
	case WARN:
		return lblWarn
	case ERROR:
		return lblError
	case FATAL:
		return lblFatal
	default:
		return lblQuiet
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
	case lblTrace:
		return TRACE
	case lblDebug:
		return DEBUG
	case lblInfo:
		return INFO
	case lblWarn:
		return WARN
	case lblError:
		return ERROR
	case lblFatal:
		return FATAL
	default:
		return QUIET
	}
}

type logger struct {
	external  log.Logger
	namespace string
	minLevel  Level
	noColor   bool
	out       io.Writer
	err       io.Writer
}

type Option func(l *logger)

func WithNoColor(b bool) Option {
	return func(l *logger) {
		l.noColor = b
	}
}

func WithMinLevel(level Level) Option {
	return func(l *logger) {
		l.minLevel = level
	}
}

func WithNamespace(namespace string) Option {
	return func(l *logger) {
		l.namespace = namespace
	}
}

func WithExternalLogger(external log.Logger) Option {
	return func(l *logger) {
		l.external = external
	}
}

func WithOutWriter(out io.Writer) Option {
	return func(l *logger) {
		l.out = out
	}
}

func WithErrWriter(err io.Writer) Option {
	return func(l *logger) {
		l.err = err
	}
}

func New(opts ...Option) *logger {
	l := &logger{
		minLevel: INFO,
		noColor:  false,
		out:      os.Stdout,
		err:      os.Stderr,
	}
	for _, o := range opts {
		o(l)
	}
	return l
}

func (l *logger) format(format string, logLevel Level) string {
	if l.noColor {
		return fmt.Sprintf(
			"%-5s %23s %26s %s\n",
			logLevel.String(),
			time.Now().Format(dateLayout),
			"["+l.namespace+"]",
			format,
		)
	}
	return fmt.Sprintf(
		"%s%-5s %23s %26s%s %s%s%s\n",
		logLevel.boldColor(),
		logLevel.String(),
		time.Now().Format(dateLayout),
		"["+l.namespace+"]",
		colorReset,
		logLevel.color(),
		format,
		colorReset,
	)
}

func (l *logger) Tracef(format string, args ...interface{}) {
	if l.minLevel > TRACE {
		return
	}
	if l.external != nil {
		l.external.Tracef(l.format(format, TRACE), args...)
	} else {
		fmt.Fprintf(l.out, l.format(format, TRACE), args...)
	}
}

func (l *logger) Debugf(format string, args ...interface{}) {
	if l.minLevel > DEBUG {
		return
	}
	if l.external != nil {
		l.external.Debugf(l.format(format, DEBUG), args...)
	} else {
		fmt.Fprintf(l.out, l.format(format, DEBUG), args...)
	}
}

func (l *logger) Infof(format string, args ...interface{}) {
	if l.minLevel > INFO {
		return
	}
	if l.external != nil {
		l.external.Infof(l.format(format, INFO), args...)
	} else {
		fmt.Fprintf(l.out, l.format(format, INFO), args...)
	}
}

func (l *logger) Warnf(format string, args ...interface{}) {
	if l.minLevel > WARN {
		return
	}
	if l.external != nil {
		l.external.Warnf(l.format(format, WARN), args...)
	} else {
		fmt.Fprintf(l.err, l.format(format, WARN), args...)
	}
}

func (l *logger) Errorf(format string, args ...interface{}) {
	if l.minLevel > ERROR {
		return
	}
	if l.external != nil {
		l.external.Errorf(l.format(format, ERROR), args...)
	} else {
		fmt.Fprintf(l.err, l.format(format, ERROR), args...)
	}
}

func (l *logger) Fatalf(format string, args ...interface{}) {
	if l.minLevel > FATAL {
		return
	}
	if l.external != nil {
		l.external.Fatalf(l.format(format, FATAL), args...)
	} else {
		fmt.Fprintf(l.err, l.format(format, FATAL), args...)
	}
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
		external:  l.external,
		out:       l.out,
		err:       l.err,
		namespace: join(l.namespace, name),
		minLevel:  l.minLevel,
		noColor:   l.noColor,
	}
}
