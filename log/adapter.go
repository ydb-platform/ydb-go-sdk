package log

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
)

type adapter struct {
	l Logger
}

func (a adapter) Log(opts logs.Options, msg string, fields ...logs.Field) {
	logger := a.l
	for _, name := range opts.Scope {
		logger = logger.WithName(name)
	}

	var f func(string, ...interface{})
	switch opts.Lvl {
	case logs.TRACE:
		f = logger.Tracef
	case logs.DEBUG:
		f = logger.Debugf
	case logs.INFO:
		f = logger.Infof
	case logs.WARN:
		f = logger.Warnf
	case logs.ERROR:
		f = logger.Errorf
	case logs.FATAL:
		f = logger.Fatalf
	case logs.QUIET:
		return
	default:
		panic("unknown log level")
	}

	fmtBldr := &strings.Builder{}
	fmtBldr.WriteString(msg)
	fmtBldr.WriteString(" {")
	values := make([]interface{}, 0, len(fields))
	for i, field := range fields {

		fmt.Fprintf(fmtBldr, `%q:%s`, field.Key(), "%s")
		if i != len(fields)-1 {
			fmtBldr.WriteByte(',')
		}
		values = append(values, field)
	}
	fmtBldr.WriteByte('}')
	f(fmtBldr.String(), values...)
}

func (a adapter) Enabled(lvl logs.Level) bool {
	return true
}

func newAdapter(l Logger) logs.Logger {
	return adapter{l: l}
}
