//go:build integration
// +build integration

package integration

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func movingAverage[T interface {
	time.Duration | float64
}](
	oldValue T, newValue T, α float64,
) T {
	return T(α*float64(newValue) + (1-α)*float64(oldValue))
}

func nameLabelValuesToString(name string, labelValues map[string]string) string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, name)
	fmt.Fprintf(&buffer, "{")
	labels := make([]string, 0, len(labelValues))
	for k := range labelValues {
		labels = append(labels, k)
	}
	sort.Strings(labels)
	for i, l := range labels {
		if i != 0 {
			fmt.Fprintf(&buffer, ",")
		}
		fmt.Fprintf(&buffer, "%s=%s", l, labelValues[l])
	}
	fmt.Fprintf(&buffer, "}")
	return buffer.String()
}

func nameLabelNamesToString(name string, labelNames []string) string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, name)
	fmt.Fprintf(&buffer, "{")
	sort.Strings(labelNames)
	for i, name := range labelNames {
		if i != 0 {
			fmt.Fprintf(&buffer, ",")
		}
		fmt.Fprintf(&buffer, name)
	}
	fmt.Fprintf(&buffer, "}")
	return buffer.String()
}

type counter struct {
	name  string
	value uint64
	m     sync.RWMutex
}

func (c *counter) Inc() {
	c.m.Lock()
	defer c.m.Unlock()
	c.value += 1
}

func (c *counter) Name() string {
	return c.name
}

func (c *counter) Value() string {
	c.m.RLock()
	defer c.m.RUnlock()
	return fmt.Sprintf("%d", c.value)
}

// define custom counterVec
type counterVec struct {
	name       string
	labelNames []string
	counters   map[string]*counter
	m          sync.RWMutex
}

func (vec *counterVec) With(labels map[string]string) metrics.Counter {
	name := nameLabelValuesToString(vec.name, labels)
	vec.m.RLock()
	c, has := vec.counters[name]
	vec.m.RUnlock()
	if has {
		return c
	}
	if has {
		return c
	}
	c = &counter{
		name: name,
	}
	vec.m.Lock()
	vec.counters[name] = c
	vec.m.Unlock()
	return c
}

func (vec *counterVec) Name() string {
	return vec.name
}

func (vec *counterVec) Values() (values []string) {
	vec.m.RLock()
	defer vec.m.RUnlock()
	for _, g := range vec.counters {
		values = append(values, fmt.Sprintf("%s = %s", g.Name(), g.Value()))
	}
	return values
}

type timer struct {
	name  string
	value time.Duration
	m     sync.RWMutex
}

func (t *timer) Record(value time.Duration) {
	t.m.Lock()
	defer t.m.Unlock()
	t.value = movingAverage(t.value, value, 0.9)
}

func (t *timer) Name() string {
	return t.name
}

func (t *timer) Value() string {
	t.m.RLock()
	defer t.m.RUnlock()
	return fmt.Sprintf("%v", t.value)
}

// define custom timerVec
type timerVec struct {
	name       string
	labelNames []string
	timers     map[string]*timer
	m          sync.RWMutex
}

func (vec *timerVec) With(labels map[string]string) metrics.Timer {
	name := nameLabelValuesToString(vec.name, labels)
	vec.m.RLock()
	t, has := vec.timers[name]
	vec.m.RUnlock()
	if has {
		return t
	}
	t = &timer{
		name: name,
	}
	vec.m.Lock()
	vec.timers[name] = t
	vec.m.Unlock()
	return t
}

func (vec *timerVec) Name() string {
	return vec.name
}

func (vec *timerVec) Values() (values []string) {
	vec.m.RLock()
	defer vec.m.RUnlock()
	for _, t := range vec.timers {
		values = append(values, fmt.Sprintf("%s = %s", t.Name(), t.Value()))
	}
	return values
}

type bin struct {
	value float64 // left corner of bucket
	count uint64
}

type histogram struct {
	name    string
	buckets []*bin
	m       sync.RWMutex
}

func (h *histogram) Record(value float64) {
	h.m.Lock()
	defer h.m.Unlock()
	for i := len(h.buckets) - 1; i >= 0; i-- {
		if h.buckets[i].value < value {
			atomic.AddUint64(&h.buckets[i].count, 1)
			return
		}
	}
}

func (h *histogram) Name() string {
	return h.name
}

func (h *histogram) Value() string {
	var buffer bytes.Buffer
	h.m.RLock()
	defer h.m.RUnlock()
	buffer.WriteByte('[')
	for i := 0; i < len(h.buckets); i++ {
		if i > 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteByte('[')
		buffer.WriteString(strconv.FormatFloat(h.buckets[i].value, 'f', -1, 64))
		buffer.WriteString("..")
		if i == len(h.buckets)-1 {
			buffer.WriteString(".")
		} else {
			buffer.WriteString(strconv.FormatFloat(h.buckets[i+1].value, 'f', -1, 64))
		}
		buffer.WriteString("]:")
		buffer.WriteString(strconv.FormatUint(atomic.LoadUint64(&h.buckets[i].count), 10))
	}
	buffer.WriteByte(']')
	return buffer.String()
}

// define custom histogramVec
type histogramVec struct {
	name       string
	labelNames []string
	histograms map[string]*histogram
	buckets    []float64
	m          sync.RWMutex
}

func (vec *histogramVec) With(labels map[string]string) metrics.Histogram {
	name := nameLabelValuesToString(vec.name, labels)
	vec.m.RLock()
	h, has := vec.histograms[name]
	vec.m.RUnlock()
	if has {
		return h
	}
	h = &histogram{
		name:    name,
		buckets: make([]*bin, len(vec.buckets)),
	}
	for i, v := range vec.buckets {
		h.buckets[i] = &bin{
			value: v,
		}
	}
	vec.m.Lock()
	vec.histograms[name] = h
	vec.m.Unlock()
	return h
}

func (vec *histogramVec) Name() string {
	return vec.name
}

func (vec *histogramVec) Values() (values []string) {
	vec.m.RLock()
	defer vec.m.RUnlock()
	for _, t := range vec.histograms {
		values = append(values, fmt.Sprintf("%s = %s", t.Name(), t.Value()))
	}
	return values
}

// define custom gaugeVec
type gaugeVec struct {
	name       string
	labelNames []string
	gauges     map[string]*gauge
	m          sync.RWMutex
}

type gauge struct {
	name  string
	value float64
	m     sync.RWMutex
}

func (g *gauge) Name() string {
	return g.name
}

func (g *gauge) Value() string {
	g.m.RLock()
	defer g.m.RUnlock()
	return fmt.Sprintf("%f", g.value)
}

func (vec *gaugeVec) With(labels map[string]string) metrics.Gauge {
	name := nameLabelValuesToString(vec.name, labels)
	vec.m.RLock()
	g, has := vec.gauges[name]
	vec.m.RUnlock()
	if has {
		return g
	}
	g = &gauge{
		name: name,
	}
	vec.m.Lock()
	vec.gauges[name] = g
	vec.m.Unlock()
	return g
}

func (g *gauge) Add(value float64) {
	g.m.Lock()
	defer g.m.Unlock()
	g.value += value
}

func (g *gauge) Set(value float64) {
	g.m.Lock()
	defer g.m.Unlock()
	g.value = value
}

func (vec *gaugeVec) Name() string {
	return vec.name
}

func (vec *gaugeVec) Values() (values []string) {
	vec.m.RLock()
	defer vec.m.RUnlock()
	for _, g := range vec.gauges {
		values = append(values, fmt.Sprintf("%s = %s", g.Name(), g.Value()))
	}
	return values
}

type vec[T any] struct {
	mtx  sync.RWMutex
	data map[string]*T
}

func newVec[T any]() *vec[T] {
	return &vec[T]{
		data: make(map[string]*T, 0),
	}
}

func (v *vec[T]) add(key string, element *T) *T {
	v.mtx.Lock()
	defer v.mtx.Unlock()
	if _, has := v.data[key]; has {
		panic(fmt.Sprintf("key = %s already exists", key))
	}
	v.data[key] = element
	return element
}

func (v *vec[T]) iterate(f func(element *T)) {
	v.mtx.RLock()
	defer v.mtx.RUnlock()
	for _, element := range v.data {
		f(element)
	}
}

// define custom registryConfig
type registryConfig struct {
	prefix     string
	details    trace.Details
	gauges     *vec[gaugeVec]
	counters   *vec[counterVec]
	timers     *vec[timerVec]
	histograms *vec[histogramVec]
}

func (registry *registryConfig) CounterVec(name string, labelNames ...string) metrics.CounterVec {
	return registry.counters.add(nameLabelNamesToString(registry.prefix+"."+name, labelNames), &counterVec{
		name:       registry.prefix + "." + name,
		labelNames: labelNames,
		counters:   make(map[string]*counter),
	})
}

func (registry *registryConfig) GaugeVec(name string, labelNames ...string) metrics.GaugeVec {
	return registry.gauges.add(nameLabelNamesToString(registry.prefix+"."+name, labelNames), &gaugeVec{
		name:       registry.prefix + "." + name,
		labelNames: labelNames,
		gauges:     make(map[string]*gauge),
	})
}

func (registry *registryConfig) TimerVec(name string, labelNames ...string) metrics.TimerVec {
	return registry.timers.add(nameLabelNamesToString(registry.prefix+"."+name, labelNames), &timerVec{
		name:       registry.prefix + "." + name,
		labelNames: labelNames,
		timers:     make(map[string]*timer),
	})
}

func (registry *registryConfig) HistogramVec(name string, buckets []float64, labelNames ...string) metrics.HistogramVec {
	return registry.histograms.add(nameLabelNamesToString(registry.prefix+"."+name, labelNames), &histogramVec{
		name:       registry.prefix + "." + name,
		labelNames: labelNames,
		histograms: make(map[string]*histogram),
		buckets:    buckets,
	})
}

func (registry *registryConfig) WithSystem(subsystem string) metrics.Config {
	copy := *registry
	if len(copy.prefix) == 0 {
		copy.prefix = subsystem
	} else {
		copy.prefix = registry.prefix + "." + subsystem
	}
	return &copy
}

func (registry *registryConfig) Details() trace.Details {
	return registry.details
}

func withMetrics(t *testing.T, details trace.Details, interval time.Duration) ydb.Option {
	registry := &registryConfig{
		details:    details,
		gauges:     newVec[gaugeVec](),
		counters:   newVec[counterVec](),
		timers:     newVec[timerVec](),
		histograms: newVec[histogramVec](),
	}
	var (
		done       = make(chan struct{})
		printState = func(log func(format string, args ...interface{}), header string) {
			log(time.Now().Format("[2006-01-02 15:04:05] ") + header + ":\n")
			var (
				gaugesOnce     sync.Once
				countersOnce   sync.Once
				timersOnce     sync.Once
				histogramsOnce sync.Once
			)
			registry.gauges.iterate(func(element *gaugeVec) {
				for _, v := range element.Values() {
					gaugesOnce.Do(func() {
						log("- gauges:\n")
					})
					log("  - %s\n", v)
				}
			})
			registry.counters.iterate(func(element *counterVec) {
				for _, v := range element.Values() {
					countersOnce.Do(func() {
						log("- counters:\n")
					})
					log("  - %s\n", v)
				}
			})
			registry.timers.iterate(func(element *timerVec) {
				for _, v := range element.Values() {
					timersOnce.Do(func() {
						log("- timers:\n")
					})
					log("  - %s\n", v)
				}
			})
			registry.histograms.iterate(func(element *histogramVec) {
				for _, v := range element.Values() {
					histogramsOnce.Do(func() {
						log("- histograms:\n")
					})
					log("  - %s\n", v)
				}
			})
		}
	)
	if interval > 0 {
		go func() {
			for {
				select {
				case <-done:
					return
				case <-time.After(interval):
					printState(t.Logf, "registry state")
				}
			}
		}()
	}
	t.Cleanup(func() {
		close(done)
		printState(func(format string, args ...interface{}) {
			_, _ = fmt.Printf(format, args...)
		}, "final registry state")
	})
	return metrics.WithTraces(registry)
}
