package metrics

import (
	"reflect"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type noopCounter struct{}

func (noopCounter) Inc() {}

type noopGauge struct{}

func (noopGauge) Add(float64) {}
func (noopGauge) Set(float64) {}

type noopTimer struct{}

func (noopTimer) Record(time.Duration) {}

type noopHistogram struct{}

func (noopHistogram) Record(float64) {}

type noopCounterVec struct{}

func (noopCounterVec) With(map[string]string) Counter { return noopCounter{} }

type noopGaugeVec struct{}

func (noopGaugeVec) With(map[string]string) Gauge { return noopGauge{} }

type noopTimerVec struct{}

func (noopTimerVec) With(map[string]string) Timer { return noopTimer{} }

type noopHistogramVec struct{}

func (noopHistogramVec) With(map[string]string) Histogram { return noopHistogram{} }

type fuzzConfig struct{}

func (fuzzConfig) Details() trace.Details { return trace.DetailsAll }

func (c fuzzConfig) WithSystem(string) Config { return c }

func (fuzzConfig) CounterVec(string, ...string) CounterVec { return noopCounterVec{} }

func (fuzzConfig) GaugeVec(string, ...string) GaugeVec { return noopGaugeVec{} }

func (fuzzConfig) TimerVec(string, ...string) TimerVec { return noopTimerVec{} }

func (fuzzConfig) HistogramVec(string, []float64, ...string) HistogramVec { return noopHistogramVec{} }

func fuzzTraces() []reflect.Value {
	config := fuzzConfig{}

	return []reflect.Value{
		reflect.ValueOf(driver(config)),
		reflect.ValueOf(table(config)),
		reflect.ValueOf(query(config)),
		reflect.ValueOf(scripting(config)),
		reflect.ValueOf(scheme(config)),
		reflect.ValueOf(coordination(config)),
		reflect.ValueOf(ratelimiter(config)),
		reflect.ValueOf(discovery(config)),
		reflect.ValueOf(DatabaseSQL(config)),
		reflect.ValueOf(retry(config)),
	}
}

func FuzzTraceHandlers(f *testing.F) {
	seed := []byte("metrics-trace-fuzz-seed")
	f.Add(seed)

	f.Fuzz(func(t *testing.T, data []byte) {
		fz := tracefuzz.New(data)
		for _, tr := range fuzzTraces() {
			tracefuzz.InvokeAll(tr, fz)
		}
	})
}

func TestFuzzTraceHandlersSeed(t *testing.T) {
	fz := tracefuzz.New([]byte("metrics-trace-fuzz-seed"))
	for _, tr := range fuzzTraces() {
		tracefuzz.InvokeAll(tr, fz)
	}
}
