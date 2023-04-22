package log

import (
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"io"
	"os"
)

const (
	dateLayout = "2006-01-02 15:04:05.000"
)

type Params struct {
	Ctx       context.Context
	Namespace []string
	Level     Level
}

func (p Params) withLevel(lvl Level) Params {
	p.Level = lvl
	return p
}

type Logger interface {
	// Log logs the message with specified options and fields.
	// Implementations must not in any way use slice of fields after Log returns.
	Log(params Params, msg string, fields ...Field)
}

type logger struct {
	namespace      []string
	logQuery       bool
	scopeMaxLen    int
	minLevel       Level
	coloring       bool
	w              io.Writer
	externalLogger Logger
	clock          clockwork.Clock
}

func (l *logger) with(opts ...Option) *logger {
	copy := *l
	for _, o := range opts {
		if o != nil {
			o(&copy)
		}
	}
	return &copy
}

func New(opts ...Option) *logger {
	l := &logger{
		scopeMaxLen: 24,
		minLevel:    INFO,
		coloring:    false,
		w:           os.Stderr,
		clock:       clockwork.NewRealClock(),
	}
	for _, o := range opts {
		if o != nil {
			o(l)
		}
	}
	return l
}

func (l *logger) format(namespace []string, msg string, logLevel Level) string {
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
	scope := joinScope(
		append(
			append(
				make([]string, 0, len(l.namespace)+len(namespace)),
				l.namespace...,
			),
			namespace...),
		l.scopeMaxLen,
	)
	for ll := len(scope); ll < l.scopeMaxLen; ll++ {
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

func (l *logger) Log(params Params, msg string, fields ...Field) {
	if params.Level < l.minLevel {
		return
	}
	if l.externalLogger != nil {
		l.externalLogger.Log(params, msg, fields...)
	} else {
		_, _ = l.w.Write([]byte(l.format(params.Namespace, appendFields(msg, fields...), params.Level)))
		_, _ = l.w.Write([]byte{'\n'})
	}
}
