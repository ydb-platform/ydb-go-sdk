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

func TestMeterAggregatesAcrossProviderInstallation(t *testing.T) {
	// The global handle accepts registrations before installation. The new
	// provider's meter also serves the callbacks delegated by that handle.
	global := &testMeter{callbacks: make(map[*metric.Callback]struct{})}
	adapter := Meter(global)
	registerSessions(t, adapter, 3)

	installed := &testMeter{callbacks: global.callbacks}
	registerSessions(t, adapter, 7)

	if got := collectValues(t, installed); !slices.Equal(got, []int64{10}) {
		t.Fatalf("observed values %v, want [10]", got)
	}
}

func TestMeterKeepsSeparateAdaptersIndependent(t *testing.T) {
	backend := &testMeter{callbacks: make(map[*metric.Callback]struct{})}
	registerSessions(t, Meter(backend), 3)
	registerSessions(t, Meter(backend), 7)

	if got := collectValues(t, backend); !slices.Equal(got, []int64{3, 7}) {
		t.Fatalf("observed values %v, want separate observations [3 7]", got)
	}
}

func registerSessions(t *testing.T, adapter telemetry.Meter, value int64) {
	t.Helper()
	unregister, err := adapter(telemetry.Descriptor{Name: "sessions"}, func(
		_ context.Context, observe func(int64, ...telemetry.Attribute),
	) error {
		observe(value)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unregister(); err != nil {
			t.Error(err)
		}
	})
}

func collectValues(t *testing.T, backend *testMeter) []int64 {
	t.Helper()
	observer := &testObserver{}
	for callback := range backend.callbacks {
		if err := (*callback)(t.Context(), observer); err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(observer.values)

	return observer.values
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
