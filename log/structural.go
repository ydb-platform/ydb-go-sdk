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
	return logger{l}
}

func (r logger) Record() structural.Record {
	rec := &record{l: r.l}
	rec.Reset()
	return rec
}

func (r logger) WithName(name string) structural.Logger {
	return Structural(r.l.WithName(name))
}

type record struct {
	lvl     Level
	formats []string
	values  []interface{}
	l       Logger
}

func (r *record) addField(key string, format string, value interface{}) *record {
	r.formats = append(r.formats, key+":"+format)
	r.values = append(r.values, value)
	return r
}

func (r *record) Level(lvl structural.Level) structural.Record {
	r.lvl = Level(lvl)
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

func (r *record) Reset() {
	r.lvl = QUIET
	r.formats = r.formats[:0]
	r.values = r.values[:0]
}

func (r *record) Message(msg string) {
	var format string
	if msg != "" {
		format = msg + " "
	}
	format += "{" + strings.Join(r.formats, ",") + "}"
	logLevel(r.l, nil, r.lvl, QUIET, format, r.values...)
}
