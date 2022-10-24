package log

import (
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/log/structural"
)

type logger struct {
	l Logger
}

func Structural(l Logger) structural.Logger {
	return &logger{l: l}
}

func (l *logger) Trace() structural.Record {
	return &record{
		send: l.l.Tracef,
	}
}

func (l *logger) Debug() structural.Record {
	return &record{
		send: l.l.Debugf,
	}
}

func (l *logger) Info() structural.Record {
	return &record{
		send: l.l.Infof,
	}
}

func (l *logger) Warn() structural.Record {
	return &record{
		send: l.l.Warnf,
	}
}

func (l *logger) Error() structural.Record {
	return &record{
		send: l.l.Errorf,
	}
}

func (l *logger) Fatal() structural.Record {
	return &record{
		send: l.l.Fatalf,
	}
}

func (l *logger) WithName(name string) structural.Logger {
	return Structural(l.l.WithName(name))
}

type record struct {
	formats []string
	values  []interface{}
	send    func(format string, a ...interface{})
}

func (r *record) addField(key string, format string, value interface{}) *record {
	r.formats = append(r.formats, key+":"+format)
	r.values = append(r.values, value)
	return r
}

func (r *record) String(key string, value string) structural.Record {
	return r.addField(key, `"%s"`, value)
}

func (r *record) Strings(key string, value []string) structural.Record {
	return r.addField(key, `%v`, value)
}

func (r *record) Duration(key string, value time.Duration) structural.Record {
	return r.addField(key, `"%v"`, value)
}

func (r *record) Error(value error) structural.Record {
	return r.addField("error", `"%v"`, value)
}

func (r *record) Message(msg string) {
	format := msg + "{" + strings.Join(r.formats, ",") + "}"
	r.send(format, r.values...)
}
