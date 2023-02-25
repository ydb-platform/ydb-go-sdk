package log

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
)

type TestLogger struct {
	out *bytes.Buffer
}

// Tracef logs at Trace logger level using fmt formatter
func (l *TestLogger) Tracef(format string, args ...interface{}) {
	fmt.Fprintf(l.out, format, args...)
}

// Debugf logs at Debug logger level using fmt formatter
func (l *TestLogger) Debugf(format string, args ...interface{}) {
	fmt.Fprintf(l.out, format, args...)
}

// Infof logs at Info logger level using fmt formatter
func (l *TestLogger) Infof(format string, args ...interface{}) {
	fmt.Fprintf(l.out, format, args...)
}

// Warnf logs at Warn logger level using fmt formatter
func (l *TestLogger) Warnf(format string, args ...interface{}) {
	fmt.Fprintf(l.out, format, args...)
}

// Errorf logs at Error logger level using fmt formatter
func (l *TestLogger) Errorf(format string, args ...interface{}) {
	fmt.Fprintf(l.out, format, args...)
}

// Fatalf logs at Fatal logger level using fmt formatter
func (l *TestLogger) Fatalf(format string, args ...interface{}) {
	fmt.Fprintf(l.out, format, args...)
}

// WithName provide applying sub-scope of logger messages
func (l *TestLogger) WithName(name string) Logger {
	return &TestLogger{}
}

type stringerTest struct{}

func (st *stringerTest) String() string {
	return "stringerTest"
}

func Test_adapter_Log(t *testing.T) {

	l := &TestLogger{out: &bytes.Buffer{}}
	//logger := newAdapter(l)
	logger := adapter{l: l}
	fields := []logs.Field{
		logs.Int("int", 1),
		logs.Int64("int64", 9223372036854775807),
		logs.String("string", "string"),
		logs.Bool("bool", true),
		logs.Duration("time.Duration", time.Hour),
		logs.Strings("[]string", []string{"Abc", "Def", "Ghi"}),
		logs.NamedError("named error", errors.New("named error")),
		logs.Error(errors.New("error")),
		logs.Any("int", 1),
		logs.Any("int64", 9223372036854775807),
		logs.Any("string", "any string"),
		logs.Any("bool", true),
		logs.Any("[]string", []string{"Abc", "Def", "Ghi"}),
		logs.Any("error", errors.New("error")),
		logs.Any("struct", struct{ str string }{str: "test"}),
		logs.Stringer("stringer", &stringerTest{}),
	}

	type args struct {
		opts   logs.Options
		msg    string
		fields []logs.Field
	}
	tests := []struct {
		name string
		a    adapter
		args args
		want string
		fail bool
	}{
		{
			name: "logs.DEBUG",
			a:    logger,
			args: args{opts: logs.Options{Lvl: logs.DEBUG}, msg: "DEBUG", fields: fields},
			want: `DEBUG {"int":1,"int64":9223372036854775807,"string":string,"bool":true,"time.Duration":1h0m0s,"[]string":[Abc Def Ghi],"named error":named error,"error":error,"int":1,"int64":9223372036854775807,"string":any string,"bool":true,"[]string":[Abc Def Ghi],"error":error,"struct":{test},"stringer":stringerTest}`,
		},
		{
			name: "logs.ERROR",
			a:    logger,
			args: args{opts: logs.Options{Lvl: logs.ERROR}, msg: "ERROR", fields: fields},
			want: `ERROR {"int":1,"int64":9223372036854775807,"string":string,"bool":true,"time.Duration":1h0m0s,"[]string":[Abc Def Ghi],"named error":named error,"error":error,"int":1,"int64":9223372036854775807,"string":any string,"bool":true,"[]string":[Abc Def Ghi],"error":error,"struct":{test},"stringer":stringerTest}`,
		},
		{
			name: "logs.FATAL",
			a:    logger,
			args: args{opts: logs.Options{Lvl: logs.FATAL}, msg: "FATAL", fields: fields},
			want: `FATAL {"int":1,"int64":9223372036854775807,"string":string,"bool":true,"time.Duration":1h0m0s,"[]string":[Abc Def Ghi],"named error":named error,"error":error,"int":1,"int64":9223372036854775807,"string":any string,"bool":true,"[]string":[Abc Def Ghi],"error":error,"struct":{test},"stringer":stringerTest}`,
		},
		{
			name: "logs.INFO",
			a:    logger,
			args: args{opts: logs.Options{Lvl: logs.INFO}, msg: "INFO", fields: fields},
			want: `INFO {"int":1,"int64":9223372036854775807,"string":string,"bool":true,"time.Duration":1h0m0s,"[]string":[Abc Def Ghi],"named error":named error,"error":error,"int":1,"int64":9223372036854775807,"string":any string,"bool":true,"[]string":[Abc Def Ghi],"error":error,"struct":{test},"stringer":stringerTest}`,
		},
		{
			name: "logs.QUIET",
			a:    logger,
			args: args{opts: logs.Options{Lvl: logs.QUIET}, msg: "QUIET", fields: fields},
			want: ``,
		},
		{
			name: "logs.TRACE",
			a:    logger,
			args: args{opts: logs.Options{Lvl: logs.TRACE}, msg: "TRACE", fields: fields},
			want: `TRACE {"int":1,"int64":9223372036854775807,"string":string,"bool":true,"time.Duration":1h0m0s,"[]string":[Abc Def Ghi],"named error":named error,"error":error,"int":1,"int64":9223372036854775807,"string":any string,"bool":true,"[]string":[Abc Def Ghi],"error":error,"struct":{test},"stringer":stringerTest}`,
		},
		{
			name: "logs.WARN",
			a:    logger,
			args: args{opts: logs.Options{Lvl: logs.WARN}, msg: "WARN", fields: fields},
			want: `WARN {"int":1,"int64":9223372036854775807,"string":string,"bool":true,"time.Duration":1h0m0s,"[]string":[Abc Def Ghi],"named error":named error,"error":error,"int":1,"int64":9223372036854775807,"string":any string,"bool":true,"[]string":[Abc Def Ghi],"error":error,"struct":{test},"stringer":stringerTest}`,
		},
		{
			name: "Unknown level",
			a:    logger,
			args: args{opts: logs.Options{Lvl: logs.Level(99)}, msg: "Unknown level", fields: fields},
			want: `unknown level`,
			fail: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.fail {
				require.Panics(t, func() { tt.a.Log(tt.args.opts, tt.args.msg, tt.args.fields...) })
				return
			}

			tt.a.Log(tt.args.opts, tt.args.msg, tt.args.fields...)
			require.Equal(t, tt.want, l.out.String())
			l.out.Truncate(0)

		})
	}
}
