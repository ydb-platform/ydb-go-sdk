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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Meter connects SDK gauge registration directly to an OTel meter.
func Meter(meter metric.Meter) telemetry.Meter {
	var mu sync.Mutex
	gauges := make(map[string]*gaugeRegistration)

	return func(desc telemetry.Descriptor, callback telemetry.Int64GaugeCallback) (func() error, error) {
		mu.Lock()
		defer mu.Unlock()

		entry := gauges[desc.Name]
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
			gauges[desc.Name] = entry
		} else {
			entry.mu.Lock()
			entry.callbacks[&callback] = struct{}{}
			entry.mu.Unlock()
		}

		return sync.OnceValue(func() error {
			mu.Lock()
			defer mu.Unlock()

			entry.mu.Lock()
			delete(entry.callbacks, &callback)
			remaining := len(entry.callbacks)
			entry.mu.Unlock()
			if remaining != 0 {
				return nil
			}
			delete(gauges, desc.Name)
			// OTel may wait for collection; do not hold the callback-set mutex here.
			if err := entry.registration.Unregister(); err != nil {
				return fmt.Errorf("unregister OTel gauge %q: %w", desc.Name, err)
			}

			return nil
		}), nil
	}
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
