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
		registry.mu.Lock()
		defer registry.mu.Unlock()

		entry := registry.gauges[key]
		if entry == nil {
			gauge, err := meter.Int64ObservableGauge(desc.Name,
				metric.WithDescription(desc.Description), metric.WithUnit(desc.Unit))
			if err != nil {
				return nil, fmt.Errorf("create OTel gauge %q: %w", desc.Name, err)
			}
			entry = &gaugeRegistration{
				gauge:     gauge,
				callbacks: make(map[*telemetry.Int64GaugeCallback]struct{}),
			}
			entry.callbacks[&callback] = struct{}{}
			reg, err := meter.RegisterCallback(entry.collect, gauge)
			if err != nil {
				return nil, fmt.Errorf("register OTel gauge %q: %w", desc.Name, err)
			}
			entry.registration = reg
			registry.gauges[key] = entry
		} else {
			entry.mu.Lock()
			entry.callbacks[&callback] = struct{}{}
			entry.mu.Unlock()
		}

		return sync.OnceValue(func() error {
			if reg := registry.detach(key, entry, &callback); reg != nil {
				// OTel may wait for collection; neither adapter lock is held.
				if err := reg.Unregister(); err != nil {
					err = fmt.Errorf("unregister OTel gauge %q: %w", desc.Name, err)
					otel.Handle(err)

					return err
				}
			}

			return nil
		}), nil
	}
}

func (r *gaugeRegistry) detach(
	key gaugeKey, entry *gaugeRegistration, callback *telemetry.Int64GaugeCallback,
) metric.Registration {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry.mu.Lock()
	defer entry.mu.Unlock()

	delete(entry.callbacks, callback)
	if len(entry.callbacks) != 0 {
		return nil
	}
	delete(r.gauges, key)

	return entry.registration
}

type gaugeRegistration struct {
	gauge        metric.Int64ObservableGauge
	registration metric.Registration

	// Collection takes only this mutex, not the registration lifecycle mutex.
	mu        sync.Mutex
	callbacks map[*telemetry.Int64GaugeCallback]struct{}
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
