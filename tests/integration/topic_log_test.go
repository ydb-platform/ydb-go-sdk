//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var internalLogger atomic.Value

func SetInternalLogger(logger *zap.Logger) {
	internalLogger.Store(logger.WithOptions(zap.AddCallerSkip(2)))
}

func Error(ctx context.Context, msg string, fields ...zap.Field) {
	logInternal(ctx, zapcore.ErrorLevel, msg, fields)
}

func logInternal(ctx context.Context, lvl zapcore.Level, msg string, fields []zap.Field) {
	lh := from(ctx)
	if lvl == zapcore.DebugLevel && lh.riseDebug {
		lvl = zapcore.InfoLevel
	}
	ce := lh.Check(lvl, msg)
	if ce == nil {
		return
	}
	ce.Write(fields...)
}

type loggerKey struct{}

// GetInternal returns context logger.
// Should be used only in integration code and with great care
// because logger is configured with zap.AddCallerSkip.
func GetInternal(ctx context.Context) *zap.Logger {
	return from(ctx).Logger
}

type loggerHolder struct {
	*zap.Logger
	riseDebug bool
}

func from(ctx context.Context) loggerHolder {
	if l, ok := ctx.Value(loggerKey{}).(loggerHolder); ok {
		return l
	}
	if l, ok := internalLogger.Load().(*zap.Logger); ok {
		return loggerHolder{Logger: l}
	}
	// Fallback, so we don't need to manually init logger in every test.
	SetInternalLogger(zap.Must(zap.NewDevelopmentConfig().Build()))
	return from(ctx)
}

func With(ctx context.Context, fields ...zap.Field) context.Context {
	lh := from(ctx)
	lh.Logger = lh.Logger.With(fields...)
	return context.WithValue(ctx, loggerKey{}, lh)
}

type SafeBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (s *SafeBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *SafeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func SetupZapLogger() (*zap.Logger, *SafeBuffer) {
	buf := SafeBuffer{}
	syncer := zapcore.AddSync(&buf)
	ws := &zapcore.BufferedWriteSyncer{
		WS:            syncer,
		Size:          512 * 1024, // 512 kB
		FlushInterval: time.Millisecond,
	}
	enc := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		MessageKey:     "M",
		LevelKey:       "L",
		TimeKey:        "T",
		NameKey:        "N",
		CallerKey:      "C",
		FunctionKey:    "F",
		StacktraceKey:  "S",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	})

	core := zapcore.NewCore(enc, ws, zapcore.DebugLevel)

	l := zap.New(core)
	SetInternalLogger(l)
	return l, &buf
}

var _ log.Logger = adapter{}

type adapter struct {
	minLevel zapcore.Level
}

func (a adapter) Log(ctx context.Context, msg string, fields ...log.Field) {
	level := Level(ctx)
	if !a.minLevel.Enabled(level) {
		return
	}
	l := GetInternal(ctx)
	for _, name := range log.NamesFromContext(ctx) {
		l = l.Named(name)
	}
	l.WithOptions(zap.AddCallerSkip(1)).Log(level, msg, Fields(fields)...)
}

//nolint:exhaustive // good enough.
func fieldToField(field log.Field) zap.Field {
	switch field.Type() {
	case log.IntType:
		return zap.Int(field.Key(), field.IntValue())
	case log.Int64Type:
		return zap.Int64(field.Key(), field.Int64Value())
	case log.StringType:
		return zap.String(field.Key(), field.StringValue())
	case log.BoolType:
		return zap.Bool(field.Key(), field.BoolValue())
	case log.DurationType:
		return zap.Duration(field.Key(), field.DurationValue())
	case log.StringsType:
		return zap.Strings(field.Key(), field.StringsValue())
	case log.ErrorType:
		return zap.Error(field.ErrorValue())
	case log.StringerType:
		return zap.Stringer(field.Key(), field.Stringer())
	default:
		return zap.Any(field.Key(), field.AnyValue())
	}
}

func Fields(fields []log.Field) []zap.Field {
	ff := make([]zap.Field, len(fields))
	for i, f := range fields {
		ff[i] = fieldToField(f)
	}
	return ff
}

//nolint:exhaustive // good enough.
func Level(ctx context.Context) zapcore.Level {
	switch log.LevelFromContext(ctx) {
	case log.TRACE, log.DEBUG:
		return zapcore.DebugLevel
	case log.INFO:
		return zapcore.InfoLevel
	case log.WARN:
		return zapcore.WarnLevel
	case log.ERROR:
		return zapcore.ErrorLevel
	case log.FATAL:
		return zapcore.FatalLevel
	default:
		return zapcore.InvalidLevel
	}
}

func WithTraces(minLevel zapcore.Level, d trace.Detailer, opts ...log.Option) ydb.Option {
	a := adapter{minLevel: minLevel}
	return ydb.MergeOptions(
		ydb.WithTraceDriver(log.Driver(a, d, opts...)),
		ydb.WithTraceTable(log.Table(a, d, opts...)),
		ydb.WithTraceScripting(log.Scripting(a, d, opts...)),
		ydb.WithTraceScheme(log.Scheme(a, d, opts...)),
		ydb.WithTraceCoordination(log.Coordination(a, d, opts...)),
		ydb.WithTraceRatelimiter(log.Ratelimiter(a, d, opts...)),
		ydb.WithTraceDiscovery(log.Discovery(a, d, opts...)),
		ydb.WithTraceTopic(log.Topic(a, d, opts...)),
		ydb.WithTraceDatabaseSQL(log.DatabaseSQL(a, d, opts...)),
	)
}

type LogEntry struct {
	Level       string `json:"L"`
	Timestamp   string `json:"T"`
	Namespace   string `json:"N"`
	Message     string `json:"M"`
	ContextName string `json:"context_name"`
	Entity      string `json:"entity"`
	Endpoint    string `json:"endpoint"`
	Method      string `json:"method"`
}

func (l *LogEntry) FormatMessageShort() string {
	return fmt.Sprintf(`"N":"%s","M":"%s"`, l.Namespace, l.Message)
}

func (l *LogEntry) FormatMessage() string {
	return fmt.Sprintf(`"N":"%s","M":"%s","context_name":"%s","entity":"%s"`, l.Namespace, l.Message, l.ContextName, l.Entity)
}

func TestTopicReadMessagesLog(t *testing.T) {
	_, buf := SetupZapLogger()

	ctx := xtest.Context(t)

	ctx = With(ctx, zap.String("context_name", "test_context"))
	ctx = With(ctx, zap.String("entity", "reader"))

	driver := connect(t, WithTraces(zapcore.DebugLevel, trace.DetailsAll))

	db, reader := createFeedAndReaderForDB(ctx, t, driver, topicoptions.WithReaderLogContext(ctx))

	sendCDCMessage(ctx, t, db)
	msg, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, msg.CreatedAt)
	t.Logf("msg: %#v", msg)

	require.NoError(t, err)
	err = topicsugar.ReadMessageDataWithCallback(msg, func(data []byte) error {
		t.Log("Content:", string(data))
		return nil
	})
	require.NoError(t, err)

	sendCDCMessage(ctx, t, db)
	batch, err := reader.ReadMessagesBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch.Messages)

	strLog := buf.String()
	strLog = strings.Replace(strLog, "\n", ",\n", -1)
	strLog = "[" + strLog[:len(strLog)-2] + "]"

	var logs []LogEntry
	err = json.Unmarshal([]byte(strLog), &logs)
	require.NoError(t, err)

	for _, logString := range logs {
		if strings.HasPrefix(logString.Namespace, "ydb.topic.reader") {
			require.Equal(t, logString.ContextName, "test_context")
			require.Equal(t, logString.Entity, "reader")
		}
	}
}
