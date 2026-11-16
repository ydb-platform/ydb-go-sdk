package spans

import (
	"context"
	"reflect"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type fuzzSpan struct{}

func (fuzzSpan) ID() (string, bool)       { return "", false }
func (fuzzSpan) TraceID() (string, bool)  { return "", false }
func (fuzzSpan) Link(Span, ...KeyValue)   {}
func (fuzzSpan) Log(string, ...KeyValue)  {}
func (fuzzSpan) Warn(error, ...KeyValue)  {}
func (fuzzSpan) Error(error, ...KeyValue) {}
func (fuzzSpan) End(...KeyValue)          {}

type fuzzAdapter struct{}

func (fuzzAdapter) Details() trace.Details { return trace.DetailsAll }

func (fuzzAdapter) SpanFromContext(context.Context) Span { return fuzzSpan{} }

func (fuzzAdapter) Start(ctx context.Context, _ string, _ ...kv.KeyValue) (context.Context, Span) {
	return ctx, fuzzSpan{}
}

func fuzzTraces() []reflect.Value {
	adapter := fuzzAdapter{}

	return []reflect.Value{
		reflect.ValueOf(driver(adapter)),
		reflect.ValueOf(table(adapter)),
		reflect.ValueOf(query(adapter)),
		reflect.ValueOf(scripting(adapter)),
		reflect.ValueOf(scheme(adapter)),
		reflect.ValueOf(coordination(adapter)),
		reflect.ValueOf(ratelimiter(adapter)),
		reflect.ValueOf(discovery(adapter)),
		reflect.ValueOf(databaseSQL(adapter)),
		reflect.ValueOf(Retry(adapter)),
	}
}

func FuzzTraceHandlers(f *testing.F) {
	seed := []byte("spans-trace-fuzz-seed")
	f.Add(seed)

	f.Fuzz(func(t *testing.T, data []byte) {
		fz := tracefuzz.New(data)
		for _, tr := range fuzzTraces() {
			tracefuzz.InvokeAll(tr, fz)
		}
	})
}

func TestFuzzTraceHandlersSeed(t *testing.T) {
	fz := tracefuzz.New([]byte("spans-trace-fuzz-seed"))
	for _, tr := range fuzzTraces() {
		tracefuzz.InvokeAll(tr, fz)
	}
}
