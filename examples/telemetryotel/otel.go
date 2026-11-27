// Package telemetryotel demonstrates direct registration with OpenTelemetry.
package telemetryotel

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type gaugeRegistry struct {
	mu     sync.Mutex
	gauges map[string]*gaugeRegistration
}

var errGaugeRetired = errors.New("OTel gauge entry retired")

// Meter connects SDK gauge registration directly to an OTel meter. Create one
// adapter per instrumentation scope and pass ydb.WithMeter(adapter) to each
// driver whose gauges should be summed together:
//
//	adapter := telemetryotel.Meter(otel.Meter("ydb"))
//	first, err := ydb.Open(ctx, firstDSN, ydb.WithMeter(adapter))
//	// Handle err before opening the second driver.
//	second, err := ydb.Open(ctx, secondDSN, ydb.WithMeter(adapter))
//
// Separate adapters register separate callbacks even on the same OTel meter.
// The OTel SDK keeps the last observation in collection order for each matching
// instrument and attribute set; it does not sum observations across callbacks.
func Meter(meter metric.Meter) telemetry.Meter {
	registry := &gaugeRegistry{gauges: make(map[string]*gaugeRegistration)}

	return func(desc telemetry.Descriptor, callback telemetry.Int64GaugeCallback) (func() error, error) {
		entry, err := registry.attach(desc.Name, &callback, func(entry *gaugeRegistration) error {
			gauge, err := meter.Int64ObservableGauge(desc.Name,
				metric.WithDescription(desc.Description), metric.WithUnit(desc.Unit))
			if err != nil {
				return fmt.Errorf("create OTel gauge %q: %w", desc.Name, err)
			}
			entry.gauge = gauge
			reg, err := meter.RegisterCallback(entry.collect, gauge)
			if err != nil {
				return fmt.Errorf("register OTel gauge %q: %w", desc.Name, err)
			}
			entry.registration = reg

			return nil
		})
		if err != nil {
			return nil, err
		}

		return sync.OnceValue(func() error {
			if err := registry.detach(desc.Name, entry, &callback); err != nil {
				err = fmt.Errorf("unregister OTel gauge %q: %w", desc.Name, err)
				otel.Handle(err)

				return err
			}

			return nil
		}), nil
	}
}

func (r *gaugeRegistry) reserve(name string) *gaugeRegistration {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry := r.gauges[name]
	if entry == nil {
		entry = &gaugeRegistration{callbacks: make(map[*telemetry.Int64GaugeCallback]struct{})}
		r.gauges[name] = entry
	}

	return entry
}

func (r *gaugeRegistry) attach(
	name string, callback *telemetry.Int64GaugeCallback, register func(*gaugeRegistration) error,
) (*gaugeRegistration, error) {
	for {
		entry, err := r.tryAttach(name, callback, register)
		if !errors.Is(err, errGaugeRetired) {
			return entry, err
		}
	}
}

func (r *gaugeRegistry) tryAttach(
	name string, callback *telemetry.Int64GaugeCallback, register func(*gaugeRegistration) error,
) (*gaugeRegistration, error) {
	entry := r.reserve(name)
	entry.lifecycle.Lock()
	defer entry.lifecycle.Unlock()

	if entry.retired {
		// The entry was retired while we waited for lifecycle.
		return nil, errGaugeRetired
	}
	entry.mu.Lock()
	entry.callbacks[callback] = struct{}{}
	entry.mu.Unlock()
	if entry.registration == nil {
		registered := false
		defer func() {
			if !registered {
				entry.remove(callback)
				r.retire(name, entry)
			}
		}()
		if err := register(entry); err != nil {
			return nil, err
		}
		registered = true
	}

	return entry, nil
}

// retire is called with the entry's lifecycle lock held.
func (r *gaugeRegistry) retire(name string, entry *gaugeRegistration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry.retired = true
	delete(r.gauges, name)
}

func (r *gaugeRegistry) detach(
	name string, entry *gaugeRegistration, callback *telemetry.Int64GaugeCallback,
) error {
	entry.lifecycle.Lock()
	defer entry.lifecycle.Unlock()

	if !entry.remove(callback) {
		return nil
	}
	// Keep the entry reserved until OTel stops using it. Collection may
	// still run while Unregister waits, so it must not take lifecycle.
	defer r.retire(name, entry)

	return entry.registration.Unregister()
}

type gaugeRegistration struct {
	lifecycle    sync.Mutex
	retired      bool
	gauge        metric.Int64ObservableGauge
	registration metric.Registration

	// Collection takes only this mutex, not the registration lifecycle mutex.
	mu        sync.Mutex
	callbacks map[*telemetry.Int64GaugeCallback]struct{}
}

func (g *gaugeRegistration) remove(callback *telemetry.Int64GaugeCallback) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.callbacks, callback)

	return len(g.callbacks) == 0
}

func (g *gaugeRegistration) snapshot() []*telemetry.Int64GaugeCallback {
	g.mu.Lock()
	defer g.mu.Unlock()

	return slices.Collect(maps.Keys(g.callbacks))
}

func (g *gaugeRegistration) collect(ctx context.Context, observer metric.Observer) error {
	type observation struct {
		attrs attribute.Set
		value int64
	}
	sums := make(map[attribute.Distinct]observation)
	var mu sync.Mutex
	observe := func(value int64, attrs ...telemetry.Attribute) {
		attributes := make([]attribute.KeyValue, 0, len(attrs))
		for _, attr := range attrs {
			attributes = append(attributes, attribute.String(attr.Key, attr.Value))
		}
		set := attribute.NewSet(attributes...)
		key := set.Equivalent()
		mu.Lock()
		defer mu.Unlock()

		sums[key] = observation{attrs: set, value: sums[key].value + value}
	}
	var err error
	for _, callback := range g.snapshot() {
		err = errors.Join(err, (*callback)(ctx, observe))
	}
	for _, sum := range sums {
		observer.ObserveInt64(g.gauge, sum.value, metric.WithAttributeSet(sum.attrs))
	}

	return err
}
