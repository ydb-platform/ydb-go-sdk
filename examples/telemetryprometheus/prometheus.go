// Package telemetryprometheus demonstrates SDK topic gauges with Prometheus.
package telemetryprometheus

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

var errEmptyAttributes = errors.New("callback must observe at least one fixed attribute set")

// Meter registers callbacks directly in the application's Prometheus registry.
// Attribute sets must be fixed for the resource lifetime, as in SDK topic gauges.
// Registration reads those attributes once; each scrape reads fresh values.
// Prometheus requires a Collector and does not wait for in-flight Collect calls
// on Unregister, so the backend-local lock protects resource teardown.
func Meter(ctx context.Context, registry prometheus.Registerer) telemetry.Meter {
	return func(desc telemetry.Descriptor, callback telemetry.Int64GaugeCallback) (func() error, error) {
		c := &collector{
			descriptor: desc,
			callback: func(observe func(int64, ...telemetry.Attribute)) error {
				return callback(ctx, observe)
			},
		}
		if err := callback(ctx, func(_ int64, attrs ...telemetry.Attribute) {
			c.descriptors = append(c.descriptors, c.describe(attrs))
		}); err != nil {
			return nil, err
		}
		if len(c.descriptors) == 0 {
			return nil, errEmptyAttributes
		}
		if err := registry.Register(c); err != nil {
			return nil, err
		}

		return func() error {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.callback != nil {
				registry.Unregister(c)
				c.callback = nil
			}

			return nil
		}, nil
	}
}

type collector struct {
	descriptor  telemetry.Descriptor
	descriptors []*prometheus.Desc
	mu          sync.RWMutex
	callback    func(func(int64, ...telemetry.Attribute)) error
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descriptors {
		ch <- desc
	}
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.callback == nil {
		return
	}
	if err := c.callback(func(value int64, attrs ...telemetry.Attribute) {
		desc := c.describe(attrs)
		metric, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, float64(value))
		if err != nil {
			ch <- prometheus.NewInvalidMetric(desc, err)

			return
		}
		ch <- metric
	}); err != nil {
		ch <- prometheus.NewInvalidMetric(c.descriptors[0], err)
	}
}

func (c *collector) describe(attrs []telemetry.Attribute) *prometheus.Desc {
	labels := make(prometheus.Labels, len(attrs))
	for _, attr := range attrs {
		labels[strings.ReplaceAll(attr.Key, ".", "_")] = attr.Value
	}

	return prometheus.NewDesc(strings.ReplaceAll(c.descriptor.Name, ".", "_"),
		c.descriptor.Description, nil, labels)
}
