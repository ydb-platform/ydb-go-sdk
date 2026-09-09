package metrics

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDriverConnectionMetrics(t *testing.T) {
	registry := newRecordingRegistry()
	tracer := driver(recordingConfig{
		registry: registry,
		details:  trace.DriverConnEvents,
	})
	ep := endpoint.New("localhost:2135", endpoint.WithID(42))
	connLabels := func(connState state.State) map[string]string {
		return map[string]string{
			"endpoint": "localhost:2135",
			"node_id":  "42",
			"state":    connState.String(),
		}
	}
	transition := func(from, to state.State) {
		done := tracer.OnConnStateChange(trace.DriverConnStateChangeStartInfo{
			Endpoint: ep,
			State:    from,
		})
		require.NotNil(t, done)
		done(trace.DriverConnStateChangeDoneInfo{State: to})
	}

	require.Equal(t, "gauge", registry.kinds["driver.conns"])
	require.Equal(t, []string{"endpoint", "node_id", "state"}, registry.labelNames["driver.conns"])
	require.Equal(t, "counter", registry.kinds["driver.conn.banned"])

	transition(state.Created, state.Online)
	transition(state.Created, state.Online)
	require.Equal(t, float64(2), registry.value("driver.conns", connLabels(state.Online)))

	transition(state.Online, state.Banned)
	require.Equal(t, float64(1), registry.value("driver.conns", connLabels(state.Online)))
	require.Equal(t, float64(1), registry.value("driver.conns", connLabels(state.Banned)))

	transition(state.Banned, state.Offline)
	transition(state.Online, state.Destroyed)
	require.Zero(t, registry.value("driver.conns", connLabels(state.Online)))
	require.Zero(t, registry.value("driver.conns", connLabels(state.Banned)))
	require.Equal(t, float64(1), registry.value("driver.conns", connLabels(state.Offline)))

	tracer.OnConnBan(trace.DriverConnBanStartInfo{
		Endpoint: ep,
		Cause:    context.Canceled,
	})
	tracer.OnConnBan(trace.DriverConnBanStartInfo{
		Endpoint: ep,
		Cause:    context.Canceled,
	})
	require.Equal(t, float64(2), registry.value("driver.conn.banned", map[string]string{
		"endpoint": "localhost:2135",
		"node_id":  "42",
		"cause":    "context/Canceled",
	}))
}

func TestDriverConnectionMetricsDisabled(t *testing.T) {
	tracer := driver(recordingConfig{
		registry: newRecordingRegistry(),
		details:  trace.DriverBalancerEvents,
	})

	require.Nil(t, tracer.OnConnStateChange(trace.DriverConnStateChangeStartInfo{}))
	require.Nil(t, tracer.OnConnBan(trace.DriverConnBanStartInfo{}))
}

type recordingRegistry struct {
	mu         sync.Mutex
	kinds      map[string]string
	labelNames map[string][]string
	values     map[string]float64
}

func newRecordingRegistry() *recordingRegistry {
	return &recordingRegistry{
		kinds:      make(map[string]string),
		labelNames: make(map[string][]string),
		values:     make(map[string]float64),
	}
}

func (r *recordingRegistry) register(path, kind string, labelNames []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.kinds[path] = kind
	r.labelNames[path] = append([]string(nil), labelNames...)
}

func (r *recordingRegistry) key(path string, labels map[string]string) string {
	parts := make([]string, 0, len(r.labelNames[path])+1)
	parts = append(parts, path)
	for _, name := range r.labelNames[path] {
		parts = append(parts, name+"="+labels[name])
	}

	return strings.Join(parts, ",")
}

func (r *recordingRegistry) add(path string, labels map[string]string, delta float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.values[r.key(path, labels)] += delta
}

func (r *recordingRegistry) set(path string, labels map[string]string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.values[r.key(path, labels)] = value
}

func (r *recordingRegistry) value(path string, labels map[string]string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.values[r.key(path, labels)]
}

type recordingCounter struct {
	registry *recordingRegistry
	path     string
	labels   map[string]string
}

func (c recordingCounter) Inc() {
	c.registry.add(c.path, c.labels, 1)
}

type recordingCounterVec struct {
	registry *recordingRegistry
	path     string
}

func (v recordingCounterVec) With(labels map[string]string) Counter {
	return recordingCounter{registry: v.registry, path: v.path, labels: labels}
}

type recordingGauge struct {
	registry *recordingRegistry
	path     string
	labels   map[string]string
}

func (g recordingGauge) Add(delta float64) {
	g.registry.add(g.path, g.labels, delta)
}

func (g recordingGauge) Set(value float64) {
	g.registry.set(g.path, g.labels, value)
}

type recordingGaugeVec struct {
	registry *recordingRegistry
	path     string
}

func (v recordingGaugeVec) With(labels map[string]string) Gauge {
	return recordingGauge{registry: v.registry, path: v.path, labels: labels}
}

type recordingConfig struct {
	registry *recordingRegistry
	system   string
	details  trace.Details
}

func (c recordingConfig) Details() trace.Details {
	return c.details
}

func (c recordingConfig) WithSystem(system string) Config {
	if c.system != "" {
		system = c.system + "." + system
	}
	c.system = system

	return c
}

func (c recordingConfig) path(name string) string {
	if c.system == "" {
		return name
	}

	return c.system + "." + name
}

func (c recordingConfig) CounterVec(name string, labelNames ...string) CounterVec {
	path := c.path(name)
	c.registry.register(path, "counter", labelNames)

	return recordingCounterVec{registry: c.registry, path: path}
}

func (c recordingConfig) GaugeVec(name string, labelNames ...string) GaugeVec {
	path := c.path(name)
	c.registry.register(path, "gauge", labelNames)

	return recordingGaugeVec{registry: c.registry, path: path}
}

func (recordingConfig) TimerVec(string, ...string) TimerVec {
	return noopTimerVec{}
}

func (recordingConfig) HistogramVec(string, []float64, ...string) HistogramVec {
	return noopHistogramVec{}
}
