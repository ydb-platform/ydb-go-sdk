package telemetryotel

import (
	"context"
	"runtime"
	"strconv"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestMeterBoundsRetainedHeapAfterAllGaugeNamesRetire(t *testing.T) {
	retired := func() int64 {
		adapter := Meter(noop.Meter{})
		func() {
			stops := make([]func() error, 100000)
			for i := range stops {
				stops[i] = registerGauge(t, adapter, "g"+strconv.Itoa(i), emptyGauge)
			}
			for _, stop := range stops {
				stopGauge(t, stop)
			}
		}()
		heap := gaugeHeap()
		runtime.KeepAlive(adapter)

		return heap
	}()
	retained := retired - gaugeHeap()
	t.Logf("empty adapter registry retained %d bytes", retained)
	if retained >= 256<<10 {
		t.Errorf("empty adapter registry retained %d bytes, want less than 256 KiB", retained)
	}
}

func registerGauge(t *testing.T, adapter telemetry.Meter, name string, callback telemetry.Int64GaugeCallback) func() error {
	t.Helper()
	stop, err := adapter(telemetry.Descriptor{Name: name}, callback)
	if err != nil {
		t.Fatal(err)
	}

	return stop
}

func stopGauge(t *testing.T, stop func() error) {
	t.Helper()
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func emptyGauge(context.Context, func(int64, ...telemetry.Attribute)) error {
	return nil
}

func gaugeHeap() int64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	return int64(stats.HeapAlloc)
}
