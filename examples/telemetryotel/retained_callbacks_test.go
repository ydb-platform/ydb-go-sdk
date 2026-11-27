package telemetryotel

import (
	"context"
	"slices"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"go.opentelemetry.io/otel/metric"
)

func TestMeterBoundsRetainedHeapWithOneGaugeCallbackLeft(t *testing.T) {
	backend := &testMeter{callbacks: make(map[*metric.Callback]struct{})}
	adapter := Meter(backend)
	const name = "anchor"
	anchor := registerGauge(t, adapter, name, func(_ context.Context, observe func(int64, ...telemetry.Attribute)) error {
		observe(7)

		return nil
	})
	func() {
		var original *metric.Callback
		for callback := range backend.callbacks {
			original = callback
		}
		stops := make([]func() error, 100000)
		for i := range stops {
			stops[i] = registerGauge(t, adapter, name, emptyGauge)
		}
		for _, stop := range stops {
			stopGauge(t, stop)
		}
		if _, ok := backend.callbacks[original]; !ok || len(backend.callbacks) != 1 {
			t.Fatal("compaction replaced the original backend registration")
		}
		if got := collectValues(t, backend); !slices.Equal(got, []int64{7}) {
			t.Fatalf("observed values %v, want [7]", got)
		}
	}()
	drained := gaugeHeap()
	stopGauge(t, anchor)
	retained := drained - gaugeHeap()
	t.Logf("single-callback entry retained %d bytes", retained)
	if retained >= 256<<10 {
		t.Errorf("single-callback entry retained %d bytes, want less than 256 KiB", retained)
	}
}
