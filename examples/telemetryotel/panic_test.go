package telemetryotel

import (
	"context"
	"runtime"
	"slices"
	"testing"
	"weak"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"go.opentelemetry.io/otel/metric"
)

func TestMeterRegistersFreshGaugeAfterBackendPanic(t *testing.T) {
	for _, tt := range []struct {
		name            string
		panicOnRegister bool
	}{
		{name: "register", panicOnRegister: true},
		{name: "unregister"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			backend := &panicMeter{testMeter: testMeter{callbacks: make(map[*metric.Callback]struct{})}}
			meter := Meter(backend)
			desc := telemetry.Descriptor{Name: "sessions"}
			backendPanic := &struct{ message string }{message: "backend panic"}
			if tt.panicOnRegister {
				backend.beforeRegister = func() { panic(backendPanic) }
			} else {
				backend.afterUnregister = func() { panic(backendPanic) }
			}

			pointer := func() weak.Pointer[[1024]byte] {
				payload := new([1024]byte)
				payload[0] = 3
				func() {
					defer func() {
						if got := recover(); got != backendPanic {
							t.Errorf("recovered %v, want original backend panic", got)
						}
					}()
					remove, err := meter(desc, func(_ context.Context, observe func(int64, ...telemetry.Attribute)) error {
						observe(int64(payload[0]))

						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
					if err := remove(); err != nil {
						t.Fatal(err)
					}
				}()

				return weak.Make(payload)
			}()
			backend.beforeRegister = nil
			backend.afterUnregister = nil
			runtime.GC()
			runtime.GC()
			if pointer.Value() != nil {
				t.Error("panicking backend operation still pins the removed collector")
			}

			remove, err := meter(desc, func(_ context.Context, observe func(int64, ...telemetry.Attribute)) error {
				observe(7)

				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := remove(); err != nil {
					t.Error(err)
				}
			})
			observer := &testObserver{}
			for callback := range backend.callbacks {
				if err := (*callback)(t.Context(), observer); err != nil {
					t.Fatal(err)
				}
			}
			if !slices.Equal(observer.values, []int64{7}) {
				t.Errorf("observed %v, want [7]", observer.values)
			}
			if backend.registrations != 2 {
				t.Errorf("backend registration attempts = %d, want 2", backend.registrations)
			}
		})
	}
}

type panicMeter struct {
	testMeter

	registrations   int
	beforeRegister  func()
	afterUnregister func()
}

func (m *panicMeter) RegisterCallback(cb metric.Callback, instruments ...metric.Observable) (
	metric.Registration, error,
) {
	m.registrations++
	if m.beforeRegister != nil {
		m.beforeRegister()
	}
	reg, err := m.testMeter.RegisterCallback(cb, instruments...)
	if err != nil {
		return nil, err
	}

	return panicRegistration{Registration: reg, meter: m}, nil
}

type panicRegistration struct {
	metric.Registration

	meter *panicMeter
}

func (r panicRegistration) Unregister() error {
	if err := r.Registration.Unregister(); err != nil {
		return err
	}
	if r.meter.afterUnregister != nil {
		r.meter.afterUnregister()
	}

	return nil
}
