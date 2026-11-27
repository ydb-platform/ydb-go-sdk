package telemetryotel

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"go.opentelemetry.io/otel/metric"
)

func TestMeterRegistersWhileAnotherMeterIsStalled(t *testing.T) {
	backend := newGatedMeter()
	release := sync.OnceFunc(func() { close(backend.resume) })
	defer release()

	waitFirst := startGaugeRegistration(t, Meter(backend), 1)
	awaitSignal(t, backend.entered, "first meter did not reach the backend")

	other := &testMeter{callbacks: make(map[*metric.Callback]struct{})}
	waitSecond := startGaugeRegistration(t, Meter(other), 2)
	waitSecond()

	release()
	waitFirst()
}

func TestMeterSharesGaugeAcrossConcurrentRegistrations(t *testing.T) {
	backend := newGatedMeter()
	adapter := Meter(backend)
	release := sync.OnceFunc(func() { close(backend.resume) })
	defer release()

	waitFirst := startGaugeRegistration(t, adapter, 3)
	awaitSignal(t, backend.entered, "first registration did not reach the backend")
	waitSecond := startGaugeRegistration(t, adapter, 7)

	release()
	waitFirst()
	waitSecond()

	if len(backend.callbacks) != 1 {
		t.Fatalf("registered %d OTel callbacks, want 1", len(backend.callbacks))
	}
	observer := &testObserver{}
	for callback := range backend.callbacks {
		if err := (*callback)(t.Context(), observer); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(observer.values, []int64{10}) {
		t.Fatalf("observed values %v, want [10]", observer.values)
	}
}

func startGaugeRegistration(t *testing.T, meter telemetry.Meter, value int64) func() {
	t.Helper()
	started, done := make(chan struct{}), make(chan struct{})
	var unregister func() error
	var err error
	go func() {
		defer close(done)
		close(started)
		unregister, err = meter(telemetry.Descriptor{Name: "sessions"}, func(
			_ context.Context, observe func(int64, ...telemetry.Attribute),
		) error {
			observe(value)

			return nil
		})
	}()
	t.Cleanup(func() {
		awaitSignal(t, done, "registration did not finish after releasing the backend")
		if unregister != nil {
			if err := unregister(); err != nil {
				t.Error(err)
			}
		}
	})
	awaitSignal(t, started, "registration goroutine did not start")

	return func() {
		t.Helper()
		awaitSignal(t, done, "gauge registration did not finish")
		if err != nil {
			t.Fatal(err)
		}
	}
}

func awaitSignal(t *testing.T, done <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(failure)
	}
}

type gatedMeter struct {
	testMeter

	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
	mu      sync.Mutex
}

func newGatedMeter() *gatedMeter {
	return &gatedMeter{
		testMeter: testMeter{callbacks: make(map[*metric.Callback]struct{})},
		entered:   make(chan struct{}),
		resume:    make(chan struct{}),
	}
}

func (m *gatedMeter) RegisterCallback(cb metric.Callback, instruments ...metric.Observable) (
	metric.Registration, error,
) {
	m.once.Do(func() { close(m.entered) })
	<-m.resume
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.testMeter.RegisterCallback(cb, instruments...)
}
