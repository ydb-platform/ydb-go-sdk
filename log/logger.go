package log

import (
	"context"
	"fmt"
	"io"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

const (
	dateLayout = "2006-01-02 15:04:05.000"
)

type Params struct {
	Ctx       context.Context
	Namespace []string
	Level     Level
}

type Logger interface {
	// Log logs the message with specified options and fields.
	// Implementations must not in any way use slice of fields after Log returns.
	Log(params Params, msg string, fields ...Field)
}

func (p Params) withLevel(lvl Level) Params {
	p.Level = lvl
	return p
}

var _ Logger = (*simpleLogger)(nil)

type simpleLoggerOption interface {
	applySimpleOption(l *simpleLogger)
}

func Simple(w io.Writer, opts ...simpleLoggerOption) *simpleLogger {
	l := &simpleLogger{
		namespaceMaxLen: 24,
		coloring:        false,
		minLevel:        INFO,
		clock:           clockwork.NewRealClock(),
		w:               w,
	}
	for _, o := range opts {
		o.applySimpleOption(l)
	}
	return l
}

type simpleLogger struct {
	namespaceMaxLen int
	coloring        bool
	clock           clockwork.Clock
	logQuery        bool
	minLevel        Level
	w               io.Writer
}

func (l *simpleLogger) format(namespace []string, msg string, logLevel Level) string {
	b := allocator.Buffers.Get()
	defer allocator.Buffers.Put(b)
	if l.coloring {
		b.WriteString(logLevel.Color())
	}
	b.WriteString(l.clock.Now().Format(dateLayout))
	b.WriteByte(' ')
	lvl := logLevel.String()
	for ll := len(lvl); ll <= 5; ll++ {
		b.WriteByte(' ')
	}
	if l.coloring {
		b.WriteString(colorReset)
		b.WriteString(logLevel.BoldColor())
	}
	b.WriteString(lvl)
	if l.coloring {
		b.WriteString(colorReset)
		b.WriteString(logLevel.Color())
	}
	scope := joinNamespace(
		namespace,
		l.namespaceMaxLen,
	)
	for ll := len(scope); ll < l.namespaceMaxLen; ll++ {
		b.WriteByte(' ')
	}
	b.WriteString(" [")
	if l.coloring {
		b.WriteString(colorReset)
		b.WriteString(logLevel.BoldColor())
	}
	b.WriteString(scope)
	if l.coloring {
		b.WriteString(colorReset)
	}
	b.WriteString("] ")
	if l.coloring {
		b.WriteString(colorReset)
		b.WriteString(logLevel.Color())
	}
	b.WriteString(msg)
	if l.coloring {
		b.WriteString(colorReset)
	}
	return b.String()
}

func (l *simpleLogger) Log(params Params, msg string, fields ...Field) {
	if params.Level < l.minLevel {
		return
	}
	_, _ = io.WriteString(l.w, l.format(params.Namespace, l.appendFields(msg, fields...), params.Level)+"\n")
}

type wrapper struct {
	namespace []string
	logQuery  bool
	logger    Logger
}

func wrapLogger(l Logger, opts ...Option) *wrapper {
	ll := &wrapper{
		logger: l,
	}
	for _, o := range opts {
		if o != nil {
			o.applyHolderOption(ll)
		}
	}
	return ll
}

func (l *simpleLogger) appendFields(msg string, fields ...Field) string {
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

func (l *wrapper) Log(params Params, msg string, fields ...Field) {
	params.Namespace = append(
		append(
			make([]string, 0, len(l.namespace)+len(params.Namespace)),
			l.namespace...,
		),
		params.Namespace...,
	)
	l.logger.Log(params, msg, fields...)
}
