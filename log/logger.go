package log

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

const (
	dateLayout = "2006-01-02 15:04:05.000"
)

type Logger interface {
	// Log logs the message with specified options and fields.
	// Implementations must not in any way use slice of fields after Log returns.
	Log(level Level, msg string, fields ...Field)

	// WithNames makes child logger with names
	WithNames(names ...string) Logger
}

type logger struct {
	namespace []string
	maxLen    int
	minLevel  Level
	coloring  bool
	w         io.Writer
}

func New(opts ...Option) *logger {
	l := &logger{
		maxLen:   24,
		minLevel: INFO,
		coloring: false,
		w:        os.Stderr,
	}
	for _, o := range opts {
		if o != nil {
			o(l)
		}
	}
	return l
}

func (l *logger) format(msg string, logLevel Level) string {
	scope := joinScope(l.namespace, l.maxLen)
	if l.coloring {
		return fmt.Sprintf(
			"%s%-5s [%"+strconv.Itoa(l.maxLen)+"s] %26s%s %s%s%s\n",
			logLevel.BoldColor(),
			logLevel.String(),
			time.Now().Format(dateLayout),
			scope,
			colorReset,
			logLevel.Color(),
			msg,
			colorReset,
		)
	}
	return fmt.Sprintf(
		"%-5s [%"+strconv.Itoa(l.maxLen)+"s] %26s %s\n",
		logLevel.String(),
		time.Now().Format(dateLayout),
		scope,
		msg,
	)
}

func (l *logger) WithNames(names ...string) Logger {
	return &logger{
		w:         l.w,
		namespace: append(l.namespace, names...),
		minLevel:  l.minLevel,
		coloring:  l.coloring,
		maxLen:    l.maxLen,
	}
}

func appendFields(msg string, fields ...Field) string {
	if len(fields) == 0 {
		return msg
	}
	b := allocator.Buffers.Get()
	defer allocator.Buffers.Put(b)
	b.WriteString(msg)
	b.WriteString(" {")
	for i, field := range fields {
		if i != 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(b, `%q:%q`, field.Key(), field.String())
	}
	b.WriteByte('}')
	return b.String()
}

func (l *logger) Log(level Level, msg string, fields ...Field) {
	if level < l.minLevel {
		return
	}
	_, _ = l.w.Write([]byte(l.format(appendFields(msg, fields...), level)))
}
