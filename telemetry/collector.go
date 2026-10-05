package telemetry

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Collector collects and reduces sources without owning an exporter.
// Its zero value is usable. Failed sources suppress their entire descriptor
// for that collection; successful, unrelated descriptors are still returned.
type Collector struct {
	mu      sync.Mutex
	sources map[*registration]struct{}
}

func NewCollector() *Collector {
	return &Collector{}
}

func (c *Collector) RegisterInt64Gauge(
	descriptor Int64GaugeDescriptor,
	source Int64GaugeSource,
) (Registration, error) {
	if descriptor.Name == "" || source == nil {
		return nil, errors.New("telemetry: gauge name and source are required")
	}
	if descriptor.Reduction != GaugeSum && descriptor.Reduction != GaugeMax {
		return nil, errors.New("telemetry: invalid gauge reduction")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for existing := range c.sources {
		if existing.descriptor.Name == descriptor.Name && existing.descriptor != descriptor {
			return nil, fmt.Errorf("telemetry: conflicting gauge descriptor %q", descriptor.Name)
		}
	}
	reg := &registration{
		collector: c, descriptor: descriptor, source: source, done: make(chan struct{}),
	}
	if c.sources == nil {
		c.sources = make(map[*registration]struct{})
	}
	c.sources[reg] = struct{}{}

	return reg, nil
}

// Collect takes one snapshot of registered sources, then invokes them outside
// the registry lock. Concurrent Close waits for this collection to release its
// sources. Returned values describe this collection, not subsequent SDK state.
func (c *Collector) Collect(ctx context.Context) ([]Int64Metric, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	sources := make([]*registration, 0, len(c.sources))
	for reg := range c.sources {
		reg.inflight++
		sources = append(sources, reg)
	}
	c.mu.Unlock()
	defer c.release(sources)

	values := make(map[Int64GaugeDescriptor]map[string]Int64Point)
	failed := make(map[Int64GaugeDescriptor]bool)
	var issues []error
	for _, reg := range sources {
		points, err := reg.source.Snapshot(ctx)
		if err != nil {
			failed[reg.descriptor] = true
			issues = append(issues, fmt.Errorf("telemetry: collect %q: %w", reg.descriptor.Name, err))

			continue
		}
		if values[reg.descriptor] == nil {
			values[reg.descriptor] = make(map[string]Int64Point)
		}
		if err = mergePoints(reg.descriptor.Reduction, values[reg.descriptor], points); err != nil {
			failed[reg.descriptor] = true
			issues = append(issues, fmt.Errorf("telemetry: collect %q: %w", reg.descriptor.Name, err))
		}
	}
	result := make([]Int64Metric, 0, len(values))
	for descriptor, points := range values {
		if failed[descriptor] {
			continue
		}
		keys := make([]string, 0, len(points))
		for key := range points {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		metric := Int64Metric{Descriptor: descriptor, Points: make([]Int64Point, 0, len(keys))}
		for _, key := range keys {
			metric.Points = append(metric.Points, points[key])
		}
		result = append(result, metric)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Descriptor.Name < result[j].Descriptor.Name
	})

	return result, errors.Join(issues...)
}

func mergePoints(reduction GaugeReduction, values map[string]Int64Point, points []Int64Point) error {
	for _, point := range points {
		key, attributes, err := pointKey(point.Attributes)
		if err != nil {
			return err
		}
		previous, exists := values[key]
		if exists {
			if reduction == GaugeSum {
				point.Value += previous.Value
			} else if previous.Value > point.Value {
				point.Value = previous.Value
			}
		}
		point.Attributes = attributes
		values[key] = point
	}

	return nil
}

func pointKey(input []Attribute) (string, []Attribute, error) {
	attributes := append([]Attribute(nil), input...)
	sort.Slice(attributes, func(i, j int) bool {
		return attributes[i].Key < attributes[j].Key
	})
	var key strings.Builder
	for i, attribute := range attributes {
		if attribute.Key == "" || (i > 0 && attributes[i-1].Key == attribute.Key) {
			return "", nil, errors.New("telemetry: attribute keys must be nonempty and unique")
		}
		key.WriteString(strconv.Itoa(len(attribute.Key)))
		key.WriteByte(':')
		key.WriteString(attribute.Key)
		key.WriteString(strconv.Itoa(len(attribute.Value)))
		key.WriteByte(':')
		key.WriteString(attribute.Value)
	}

	return key.String(), attributes, nil
}

type registration struct {
	collector  *Collector
	descriptor Int64GaugeDescriptor
	source     Int64GaugeSource
	done       chan struct{}
	inflight   int
	closed     bool
}

func (r *registration) Close(ctx context.Context) error {
	r.collector.mu.Lock()
	if !r.closed {
		r.closed = true
		delete(r.collector.sources, r)
		if r.inflight == 0 {
			close(r.done)
		}
	}
	r.collector.mu.Unlock()
	select {
	case <-r.done:
		return nil
	default:
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Collector) release(sources []*registration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, reg := range sources {
		reg.inflight--
		if reg.closed && reg.inflight == 0 {
			close(reg.done)
		}
	}
}
