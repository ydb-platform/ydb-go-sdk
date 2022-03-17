package logger

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/logger/level"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
)

const (
	colorReset = "\033[0m"

	dateLayout = "2006-01-02 15:04:05.000"
)

type logger struct {
	external  log.Logger
	namespace string
	minLevel  level.Level
	noColor   bool
	out       io.Writer
	err       io.Writer
}

func New(opts ...Option) *logger {
	l := &logger{
		minLevel: level.INFO,
		noColor:  false,
		out:      os.Stdout,
		err:      os.Stderr,
	}
	for _, o := range opts {
		o(l)
	}
	return l
}

func (l *logger) format(format string, logLevel level.Level) string {
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
		logLevel.BoldColor(),
		logLevel.String(),
		time.Now().Format(dateLayout),
		"["+l.namespace+"]",
		colorReset,
		logLevel.Color(),
		format,
		colorReset,
	)
}

func (l *logger) Tracef(format string, args ...interface{}) {
	if l.minLevel > level.TRACE {
		return
	}
	if l.external != nil {
		l.external.Tracef(l.format(format, level.TRACE), args...)
	} else {
		fmt.Fprintf(l.out, l.format(format, level.TRACE), args...)
	}
}

func (l *logger) Debugf(format string, args ...interface{}) {
	if l.minLevel > level.DEBUG {
		return
	}
	if l.external != nil {
		l.external.Debugf(l.format(format, level.DEBUG), args...)
	} else {
		fmt.Fprintf(l.out, l.format(format, level.DEBUG), args...)
	}
}

func (l *logger) Infof(format string, args ...interface{}) {
	if l.minLevel > level.INFO {
		return
	}
	if l.external != nil {
		l.external.Infof(l.format(format, level.INFO), args...)
	} else {
		fmt.Fprintf(l.out, l.format(format, level.INFO), args...)
	}
}

func (l *logger) Warnf(format string, args ...interface{}) {
	if l.minLevel > level.WARN {
		return
	}
	if l.external != nil {
		l.external.Warnf(l.format(format, level.WARN), args...)
	} else {
		fmt.Fprintf(l.err, l.format(format, level.WARN), args...)
	}
}

func (l *logger) Errorf(format string, args ...interface{}) {
	if l.minLevel > level.ERROR {
		return
	}
	if l.external != nil {
		l.external.Errorf(l.format(format, level.ERROR), args...)
	} else {
		fmt.Fprintf(l.err, l.format(format, level.ERROR), args...)
	}
}

func (l *logger) Fatalf(format string, args ...interface{}) {
	if l.minLevel > level.FATAL {
		return
	}
	if l.external != nil {
		l.external.Fatalf(l.format(format, level.FATAL), args...)
	} else {
		fmt.Fprintf(l.err, l.format(format, level.FATAL), args...)
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
