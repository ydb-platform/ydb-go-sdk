package log

import (
	"context"
	"reflect"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type fuzzLogger struct{}

func (fuzzLogger) Log(_ context.Context, _ string, _ ...Field) {}

func fuzzTraces() []reflect.Value {
	logger := fuzzLogger{}
	details := trace.DetailsAll

	return []reflect.Value{
		reflect.ValueOf(Driver(logger, details)),
		reflect.ValueOf(Table(logger, details)),
		reflect.ValueOf(Query(logger, details)),
		reflect.ValueOf(Scripting(logger, details)),
		reflect.ValueOf(Scheme(logger, details)),
		reflect.ValueOf(Coordination(logger, details)),
		reflect.ValueOf(Ratelimiter(logger, details)),
		reflect.ValueOf(Discovery(logger, details)),
		reflect.ValueOf(Topic(logger, details)),
		reflect.ValueOf(DatabaseSQL(logger, details)),
		reflect.ValueOf(Retry(logger, details)),
	}
}

func FuzzTraceHandlers(f *testing.F) {
	seed := []byte("log-trace-fuzz-seed")
	f.Add(seed)

	f.Fuzz(func(t *testing.T, data []byte) {
		fz := tracefuzz.New(data)
		for _, tr := range fuzzTraces() {
			tracefuzz.InvokeAll(tr, fz)
		}
	})
}

func TestFuzzTraceHandlersSeed(t *testing.T) {
	fz := tracefuzz.New([]byte("log-trace-fuzz-seed"))
	for _, tr := range fuzzTraces() {
		tracefuzz.InvokeAll(tr, fz)
	}
}
