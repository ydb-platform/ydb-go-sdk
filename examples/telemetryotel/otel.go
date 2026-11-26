// Package telemetryotel demonstrates direct registration with OpenTelemetry.
package telemetryotel

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type gaugeKey struct {
	meter metric.Meter
	name  string
}

type gaugeRegistry struct {
	mu     sync.Mutex
	gauges map[gaugeKey]*gaugeRegistration
}

var sharedGauges = gaugeRegistry{gauges: make(map[gaugeKey]*gaugeRegistration)}

var errGaugeRetired = errors.New("OTel gauge entry retired")

// Meter connects SDK gauge registration directly to an OTel meter. Adapters
// wrapping the same comparable meter (including SDK meters) share registrations.
// For a non-comparable meter implementation, share the returned adapter instead.
func Meter(meter metric.Meter) telemetry.Meter {
	registry := &sharedGauges
	identity := meter
	if !reflect.ValueOf(meter).Comparable() {
		registry = &gaugeRegistry{gauges: make(map[gaugeKey]*gaugeRegistration)}
		identity = nil
	}

	return func(desc telemetry.Descriptor, callback telemetry.Int64GaugeCallback) (func() error, error) {
		key := gaugeKey{meter: identity, name: desc.Name}
		entry, err := registry.attach(key, &callback, func(entry *gaugeRegistration) error {
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
			if err := registry.detach(key, entry, &callback); err != nil {
				err = fmt.Errorf("unregister OTel gauge %q: %w", desc.Name, err)
				otel.Handle(err)

				return err
			}

			return nil
		}), nil
	}
}

func (r *gaugeRegistry) reserve(key gaugeKey) *gaugeRegistration {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry := r.gauges[key]
	if entry == nil {
		entry = &gaugeRegistration{callbacks: make(map[*telemetry.Int64GaugeCallback]struct{})}
		r.gauges[key] = entry
	}

	return entry
}

func (r *gaugeRegistry) attach(
	key gaugeKey, callback *telemetry.Int64GaugeCallback, register func(*gaugeRegistration) error,
) (*gaugeRegistration, error) {
	for {
		entry, err := r.tryAttach(key, callback, register)
		if !errors.Is(err, errGaugeRetired) {
			return entry, err
		}
	}
}

func (r *gaugeRegistry) tryAttach(
	key gaugeKey, callback *telemetry.Int64GaugeCallback, register func(*gaugeRegistration) error,
) (*gaugeRegistration, error) {
	entry := r.reserve(key)
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
		if err := register(entry); err != nil {
			entry.remove(callback)
			r.retire(key, entry)

			return nil, err
		}
	}

	return entry, nil
}

// retire is called with the entry's lifecycle lock held.
func (r *gaugeRegistry) retire(key gaugeKey, entry *gaugeRegistration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry.retired = true
	delete(r.gauges, key)
}

func (r *gaugeRegistry) detach(
	key gaugeKey, entry *gaugeRegistration, callback *telemetry.Int64GaugeCallback,
) error {
	entry.lifecycle.Lock()
	defer entry.lifecycle.Unlock()

	if !entry.remove(callback) {
		return nil
	}
	// Keep the entry reserved until OTel stops using it. Collection may
	// still run while Unregister waits, so it must not take lifecycle.
	err := entry.registration.Unregister()
	r.retire(key, entry)

	return err
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
