package log

import (
	"fmt"
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

func (l *logger) Object() structural.Record {
	return &record{}
}

func (l *logger) Array() structural.Array {
	return &array{}
}

type record struct {
	formats []string
	values  []interface{}
	send    func(format string, a ...interface{})
}

func (r *record) addField(key string, format string, value interface{}) *record {
	r.formats = append(r.formats, fmt.Sprintf("%q:%s", key, format))
	r.values = append(r.values, value)
	return r
}

func (r *record) format() string {
	return "{" + strings.Join(r.formats, ",") + "}"
}

func (r *record) Object(key string, value structural.Record) structural.Record {
	rec, ok := value.(*record)
	if !ok {
		panic("ydb: unsupported Record")
	}
	r.formats = append(r.formats, fmt.Sprintf("%q:%s", key, rec.format()))
	r.values = append(r.values, rec.values...)
	return r
}

func (r *record) Array(key string, value structural.Array) structural.Record {
	arr, ok := value.(*array)
	if !ok {
		panic("ydb: unsupported Array")
	}
	r.formats = append(r.formats, fmt.Sprintf("%q:%s", key, arr.format()))
	r.values = append(r.values, arr.values...)
	return r
}

func (r *record) String(key string, value string) structural.Record {
	return r.addField(key, `"%s"`, value)
}

func (r *record) Strings(key string, value []string) structural.Record {
	return r.addField(key, `%v`, value)
}

func (r *record) Stringer(key string, value fmt.Stringer) structural.Record {
	return r.String(key, value.String())
}

func (r *record) Duration(key string, value time.Duration) structural.Record {
	return r.addField(key, `"%v"`, value)
}

func (r *record) Int(key string, value int) structural.Record {
	return r.addField(key, `%d`, value)
}

func (r *record) Int8(key string, value int8) structural.Record {
	return r.addField(key, `%d`, value)
}

func (r *record) Int16(key string, value int16) structural.Record {
	return r.addField(key, `%d`, value)
}

func (r *record) Int32(key string, value int32) structural.Record {
	return r.addField(key, `%d`, value)
}

func (r *record) Int64(key string, value int64) structural.Record {
	return r.addField(key, `%d`, value)
}

func (r *record) Uint(key string, value uint) structural.Record {
	return r.addField(key, `%d`, value)
}

func (r *record) Uint8(key string, value uint8) structural.Record {
	return r.addField(key, `%d`, value)
}

func (r *record) Uint16(key string, value uint16) structural.Record {
	return r.addField(key, `%d`, value)
}

func (r *record) Uint32(key string, value uint32) structural.Record {
	return r.addField(key, `%d`, value)
}

func (r *record) Uint64(key string, value uint64) structural.Record {
	return r.addField(key, `%d`, value)
}

func (r *record) Float32(key string, value float32) structural.Record {
	return r.addField(key, `%f`, value)
}

func (r *record) Float64(key string, value float64) structural.Record {
	return r.addField(key, `%f`, value)
}

func (r *record) Bool(key string, value bool) structural.Record {
	return r.addField(key, `%t`, value)
}

func (r *record) Error(value error) structural.Record {
	return r.NamedError("error", value)
}

func (r *record) NamedError(key string, value error) structural.Record {
	return r.addField(key, `"%v"`, value)
}

func (r *record) Any(key string, value interface{}) structural.Record {
	return r.addField(key, `"%v"`, value)
}

func (r *record) Message(msg string) {
	if r.send == nil {
		return
	}
	format := msg + r.format()
	r.send(format, r.values...)
}

type array struct {
	formats []string
	values  []interface{}
}

func (a *array) addItem(format string, value interface{}) *array {
	a.formats = append(a.formats, format)
	a.values = append(a.values, value)
	return a
}

func (a *array) format() string {
	return "[" + strings.Join(a.formats, ",") + "]"
}

func (a *array) Object(value structural.Record) structural.Array {
	rec, ok := value.(*record)
	if !ok {
		panic("ydb: unsupported Record")
	}
	a.formats = append(a.formats, rec.format())
	a.values = append(a.values, rec.values...)
	return a
}

func (a *array) Array(value structural.Array) structural.Array {
	arr, ok := value.(*array)
	if !ok {
		panic("ydb: unsupported Array")
	}
	a.formats = append(a.formats, arr.format())
	a.values = append(a.values, arr.values...)
	return a
}

func (a *array) String(value string) structural.Array {
	return a.addItem(`"%s"`, value)
}

func (a *array) Strings(value []string) structural.Array {
	return a.addItem(`%v`, value)
}

func (a *array) Stringer(value fmt.Stringer) structural.Array {
	return a.String(value.String())
}

func (a *array) Duration(value time.Duration) structural.Array {
	return a.addItem(`"%v"`, value)
}

func (a *array) Int(value int) structural.Array {
	return a.addItem(`%d`, value)
}

func (a *array) Int8(value int8) structural.Array {
	return a.addItem(`%d`, value)
}

func (a *array) Int16(value int16) structural.Array {
	return a.addItem(`%d`, value)
}

func (a *array) Int32(value int32) structural.Array {
	return a.addItem(`%d`, value)
}

func (a *array) Int64(value int64) structural.Array {
	return a.addItem(`%d`, value)
}

func (a *array) Uint(value uint) structural.Array {
	return a.addItem(`%d`, value)
}

func (a *array) Uint8(value uint8) structural.Array {
	return a.addItem(`%d`, value)
}

func (a *array) Uint16(value uint16) structural.Array {
	return a.addItem(`%d`, value)
}

func (a *array) Uint32(value uint32) structural.Array {
	return a.addItem(`%d`, value)
}

func (a *array) Uint64(value uint64) structural.Array {
	return a.addItem(`%d`, value)
}

func (a *array) Float32(value float32) structural.Array {
	return a.addItem(`%f`, value)
}

func (a *array) Float64(value float64) structural.Array {
	return a.addItem(`%f`, value)
}

func (a *array) Bool(value bool) structural.Array {
	return a.addItem(`%t`, value)
}

func (a *array) Any(value interface{}) structural.Array {
	return a.addItem(`"%v"`, value)
}
