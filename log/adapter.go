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
	default:
		return
	}

	fmtBldr := &strings.Builder{}
	fmtBldr.WriteString(msg)
	fmtBldr.WriteString(" {")
	values := make([]interface{}, 0, len(fields))
	for i, field := range fields {
		var (
			format string
			value  interface{}
		)
		//nolint:goconst
		switch field.Type() {
		case logs.InvalidType:
			continue
		case logs.IntType:
			format = "%d"
			value = field.Int()
		case logs.Int64Type:
			format = "%d"
			value = field.Int64()
		case logs.StringType:
			format = "%s"
			value = field.String()
		case logs.BoolType:
			format = "%t"
			value = field.Bool()
		case logs.DurationType:
			format = "%v"
			value = field.Duration()
		case logs.StringsType:
			format = "%v"
			value = field.Strings()
		case logs.ErrorType:
			format = "%v"
			value = field.Error()
		case logs.AnyType:
			format = "%v"
			value = field.Any()
		case logs.StringerType:
			format = "%s"
			value = field.Stringer().String()
		default:
			if fb, err := field.Fallback(); err != nil {
				format = "<error:%q>"
				value = err
			} else {
				format = "%s"
				value = fb
			}
		}
		fmt.Fprintf(fmtBldr, `%q:%s`, field.Key(), format)
		if i != len(fields)-1 {
			fmtBldr.WriteByte(',')
		}
		values = append(values, value)
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
