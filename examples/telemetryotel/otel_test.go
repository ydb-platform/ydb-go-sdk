package telemetryotel

import (
	"context"
	"slices"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestMeterAggregatesAndUnregistersGauge(t *testing.T) {
	backend := &testMeter{callbacks: make(map[*metric.Callback]struct{})}
	meter := Meter(backend)
	desc := telemetry.Descriptor{Name: "sessions"}
	register := func(value int64, attrs ...telemetry.Attribute) func() error {
		t.Helper()
		remove, err := meter(desc, func(_ context.Context, observe func(int64, ...telemetry.Attribute)) error {
			observe(value, attrs...)

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		return remove
	}
	check := func(want ...int64) {
		t.Helper()
		observer := &testObserver{}
		for callback := range backend.callbacks {
			if err := (*callback)(t.Context(), observer); err != nil {
				t.Fatal(err)
			}
		}
		if !slices.Equal(observer.values, want) {
			t.Fatalf("observed values %v, want %v", observer.values, want)
		}
	}
	unregister := func(remove func() error) {
		t.Helper()
		if err := remove(); err != nil {
			t.Fatal(err)
		}
	}

	name := telemetry.Attribute{Key: "reader.name", Value: "default"}
	topic := telemetry.Attribute{Key: "topic", Value: "/local/topic"}
	active, idle := register(3, name, topic), register(0, topic, name)
	check(3)
	unregister(idle)
	unregister(idle)
	check(3)
	unregister(active)
	check()
	if len(backend.callbacks) != 0 {
		t.Fatal("last unregister left an OTel callback attached")
	}
	again := register(7, name, topic)
	check(7)
	unregister(again)
}

type testMeter struct {
	noop.Meter

	callbacks map[*metric.Callback]struct{}
}

func (m *testMeter) RegisterCallback(cb metric.Callback, _ ...metric.Observable) (metric.Registration, error) {
	m.callbacks[&cb] = struct{}{}

	return testRegistration{remove: func() { delete(m.callbacks, &cb) }}, nil
}

type testRegistration struct {
	noop.Registration

	remove func()
}

func (r testRegistration) Unregister() error {
	r.remove()

	return nil
}

type testObserver struct {
	noop.Observer

	values []int64
}

func (o *testObserver) ObserveInt64(_ metric.Int64Observable, value int64, _ ...metric.ObserveOption) {
	o.values = append(o.values, value)
}
