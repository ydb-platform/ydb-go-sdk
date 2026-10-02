package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicReaderMetricDescriptorsAndLegacyNames(t *testing.T) {
	descriptorRegistry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	topic(topicAddCounterConfig{
		recordingConfig: recordingConfig{
			registry: descriptorRegistry,
			system:   "custom",
			details:  trace.DetailsAll,
		},
		capture: capture,
	})

	legacyRegistry := newRecordingRegistry()
	topic(recordingConfig{registry: legacyRegistry, system: "custom", details: trace.DetailsAll})

	expected := []topicMetricDescriptor{
		{
			name: "ydb.topic.reader.received.messages", legacyName: "custom.topic.reader.received.messages",
			unit: "{message}", kind: "counter", labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		{
			name: "ydb.topic.reader.delivered.messages", legacyName: "custom.topic.reader.delivered.messages",
			unit: "{message}", kind: "counter", labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		{
			name: "ydb.topic.reader.received.bytes", legacyName: "custom.topic.reader.received.bytes",
			unit: "By", kind: "counter", labels: []string{"endpoint", "database", "consumer", "reader.name"},
		},
		{
			name: "ydb.topic.reader.session.errors", legacyName: "custom.topic.reader.session.errors",
			unit: "{error}", kind: "counter",
			labels: []string{"endpoint", "database", "consumer", "reader.name", "retry_decision", "status_code", "error.type"},
		},
		{
			name: "ydb.topic.reader.commit.queued", legacyName: "custom.topic.reader.commit.queued",
			unit: "{message}", kind: "counter", labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		{
			name: "ydb.topic.reader.commit.acknowledged", legacyName: "custom.topic.reader.commit.acknowledged",
			unit: "{message}", kind: "counter", labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		{
			name: "ydb.topic.reader.local_buffer.messages", legacyName: "custom.topic.reader.local_buffer.messages",
			unit: "{message}", kind: "gauge", labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		{
			name: "ydb.topic.reader.credit_balance_bytes", legacyName: "custom.topic.reader.credit_balance_bytes",
			unit: "By", kind: "gauge", labels: []string{"endpoint", "database", "consumer", "reader.name"},
		},
	}

	for _, metric := range expected {
		require.Equal(t, metric.unit, capture.units[metric.name], metric.name)
		require.Equal(t, metric.kind, descriptorRegistry.kinds[metric.name], metric.name)
		require.Equal(t, metric.labels, descriptorRegistry.labelNames[metric.name], metric.name)
		require.Equal(t, metric.kind, legacyRegistry.kinds[metric.legacyName], metric.legacyName)
		require.Equal(t, metric.labels, legacyRegistry.labelNames[metric.legacyName], metric.legacyName)
	}
	require.Len(t, capture.units, len(expected))
	require.NotContains(t, legacyRegistry.kinds, "custom.topic.reader.credit_balance.bytes")
}

func TestDisabledTopicMetricsDoNotEnableTracking(t *testing.T) {
	tracer := topic(recordingConfig{registry: newRecordingRegistry(), details: trace.TopicReaderCustomerEvents})
	require.Nil(t, tracer.OnReaderMessagesReceived)
	require.Nil(t, tracer.OnReaderMessagesDelivered)
	require.Nil(t, tracer.OnReaderReceivedBytes)
	require.Nil(t, tracer.OnReaderLocalBufferChanged)
	require.Nil(t, tracer.OnReaderCreditBalanceChanged)
	require.Nil(t, tracer.OnReaderCommitQueued)
	require.Nil(t, tracer.OnReaderCommitAcknowledged)
	require.Nil(t, tracer.OnReaderSessionError)
}

func TestTopicReaderMetricsHonorDetailsGroups(t *testing.T) {
	messageEvents := topic(recordingConfig{registry: newRecordingRegistry(), details: trace.TopicReaderMessageEvents})
	require.NotNil(t, messageEvents.OnReaderMessagesReceived)
	require.NotNil(t, messageEvents.OnReaderMessagesDelivered)
	require.NotNil(t, messageEvents.OnReaderReceivedBytes)
	require.NotNil(t, messageEvents.OnReaderLocalBufferChanged)
	require.NotNil(t, messageEvents.OnReaderCreditBalanceChanged)
	require.Nil(t, messageEvents.OnReaderCommitQueued)
	require.Nil(t, messageEvents.OnReaderCommitAcknowledged)
	require.Nil(t, messageEvents.OnReaderSessionError)

	streamEvents := topic(recordingConfig{registry: newRecordingRegistry(), details: trace.TopicReaderStreamEvents})
	require.Nil(t, streamEvents.OnReaderMessagesReceived)
	require.Nil(t, streamEvents.OnReaderMessagesDelivered)
	require.Nil(t, streamEvents.OnReaderReceivedBytes)
	require.Nil(t, streamEvents.OnReaderLocalBufferChanged)
	require.Nil(t, streamEvents.OnReaderCreditBalanceChanged)
	require.NotNil(t, streamEvents.OnReaderCommitQueued)
	require.NotNil(t, streamEvents.OnReaderCommitAcknowledged)
	require.NotNil(t, streamEvents.OnReaderSessionError)
}

func TestTopicReaderCountersIgnoreNonPositiveDeltas(t *testing.T) {
	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	tracer := topic(topicAddCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.DetailsAll},
		capture:         capture,
	})
	for _, count := range []int{0, -1} {
		tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{MessagesCount: count})
		tracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{MessagesCount: count})
		tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{Bytes: count})
		tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{MessagesCount: count})
		tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{MessagesCount: count})
	}
	require.Empty(t, capture.adds)
	require.Empty(t, registry.values)
}

func TestTopicReaderCountersRecordLabeledValues(t *testing.T) {
	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	messageTracer := topic(topicAddCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.TopicReaderMessageEvents},
		capture:         capture,
	})
	streamTracer := topic(topicAddCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.TopicReaderStreamEvents},
		capture:         capture,
	})
	messageLabels := map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}
	streamLabels := map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
	}
	errorLabels := map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
		"retry_decision": "retry", "status_code": "UNAVAILABLE", "error.type": "transport_error",
	}

	messageTracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer", ReaderName: "reader", MessagesCount: 4,
	})
	messageTracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer", ReaderName: "reader", MessagesCount: 3,
	})
	messageTracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: "reader", Bytes: 12,
	})
	streamTracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer", ReaderName: "reader", MessagesCount: 2,
	})
	streamTracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer", ReaderName: "reader", MessagesCount: 1,
	})
	streamTracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: "reader",
		RetryDecision: "retry", StatusCode: "UNAVAILABLE", ErrorType: "transport_error",
	})

	require.Equal(t, map[string][]int64{
		"ydb.topic.reader.received.messages":   {4},
		"ydb.topic.reader.delivered.messages":  {3},
		"ydb.topic.reader.received.bytes":      {12},
		"ydb.topic.reader.commit.queued":       {2},
		"ydb.topic.reader.commit.acknowledged": {1},
		"ydb.topic.reader.session.errors":      {1},
	}, capture.adds)
	require.Equal(t, float64(4), registry.value("ydb.topic.reader.received.messages", messageLabels))
	require.Equal(t, float64(3), registry.value("ydb.topic.reader.delivered.messages", messageLabels))
	require.Equal(t, float64(2), registry.value("ydb.topic.reader.commit.queued", messageLabels))
	require.Equal(t, float64(1), registry.value("ydb.topic.reader.commit.acknowledged", messageLabels))
	require.Equal(t, float64(12), registry.value("ydb.topic.reader.received.bytes", streamLabels))
	require.Equal(t, float64(1), registry.value("ydb.topic.reader.session.errors", errorLabels))
}

func TestTopicReaderCountersSupportLegacyCounterConfig(t *testing.T) {
	registry := newRecordingRegistry()
	tracer := topic(recordingConfig{registry: registry, system: "custom", details: trace.DetailsAll})
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: "reader", MessagesCount: 3,
	})
	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer",
		ReaderName: "reader", Bytes: 1,
	})
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: "reader", MessagesCount: 1,
	})
	tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: "reader", MessagesCount: 1,
	})
	tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: "reader",
		RetryDecision: "retry", StatusCode: "UNAVAILABLE", ErrorType: "transport_error",
	})

	require.Len(t, registry.values, 2)
	require.Equal(t, float64(3), registry.value("custom.topic.reader.received.messages", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Zero(t, registry.value("custom.topic.reader.received.bytes", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Zero(t, registry.value("custom.topic.reader.commit.queued", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Zero(t, registry.value("custom.topic.reader.commit.acknowledged", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Equal(t, float64(1), registry.value("custom.topic.reader.session.errors", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
		"retry_decision": "retry", "status_code": "UNAVAILABLE", "error.type": "transport_error",
	}))
}

func TestTopicReaderCountersUseInt64AddForLargeDeltas(t *testing.T) {
	largeDelta := int(^uint(0) >> 1)

	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	tracer := topic(topicAddCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.DetailsAll},
		capture:         capture,
	})

	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: "reader", Bytes: largeDelta,
	})
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		Endpoint:      "node",
		Database:      "/db",
		Topic:         "/topic",
		Consumer:      "consumer",
		ReaderName:    "reader",
		MessagesCount: largeDelta,
	})
	tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{
		Endpoint:      "node",
		Database:      "/db",
		Topic:         "/topic",
		Consumer:      "consumer",
		ReaderName:    "reader",
		MessagesCount: largeDelta,
	})

	require.Equal(t, map[string][]int64{
		"ydb.topic.reader.received.bytes":      {int64(largeDelta)},
		"ydb.topic.reader.commit.queued":       {int64(largeDelta)},
		"ydb.topic.reader.commit.acknowledged": {int64(largeDelta)},
	}, capture.adds)
}

func TestTopicReaderGaugesApplyDeltasWithoutSet(t *testing.T) {
	registry := newRecordingRegistry()
	capture := &topicGaugeCapture{}
	tracer := topic(topicTrackingGaugeConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.TopicReaderMessageEvents},
		capture:         capture,
	})

	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: "reader", MessagesDelta: 5,
	})
	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: "reader", MessagesDelta: -2,
	})
	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{MessagesDelta: 0})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: "reader", BytesDelta: 100,
	})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: "reader", BytesDelta: -40,
	})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: 0})

	require.Equal(t, []float64{5, -2}, capture.adds["topic.reader.local_buffer.messages"])
	require.Equal(t, []float64{100, -40}, capture.adds["topic.reader.credit_balance_bytes"])
	require.Empty(t, capture.sets)
	require.Equal(t, float64(3), registry.value("topic.reader.local_buffer.messages", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Equal(t, float64(60), registry.value("topic.reader.credit_balance_bytes", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
	}))
}

func TestTopicMetricCallbacksProvideAllDeclaredLabels(t *testing.T) {
	for _, test := range []struct {
		name       string
		consumer   string
		readerName string
	}{
		{name: "without consumer or reader name"},
		{name: "without consumer with reader name", readerName: "generated-reader"},
		{name: "with consumer and reader name", consumer: "consumer", readerName: "reader"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracer := topic(strictTopicConfig{
				recordingConfig: recordingConfig{details: trace.DetailsAll},
				t:               t,
			})

			tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
				Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: test.consumer,
				ReaderName: test.readerName, MessagesCount: 1,
			})
			tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
				Endpoint: "node", Database: "/db", Consumer: test.consumer,
				ReaderName: test.readerName, Bytes: 1,
			})
			tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
				Endpoint: "node", Database: "/db", Consumer: test.consumer, ReaderName: test.readerName,
				RetryDecision: "retry", StatusCode: "UNAVAILABLE", ErrorType: "transport_error",
			})
			tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
				Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: test.consumer,
				ReaderName: test.readerName, MessagesDelta: 1,
			})
			tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
				Endpoint: "node", Database: "/db", Consumer: test.consumer,
				ReaderName: test.readerName, BytesDelta: 1,
			})
		})
	}
}

func TestTopicMetricsSeparateReaderAndListenerDetails(t *testing.T) {
	for _, test := range []struct {
		name    string
		details trace.Details
		want    float64
	}{
		{"reader", trace.TopicReaderEvents, 1},
		{"listener", trace.TopicListenerEvents, 10},
		{"both", trace.TopicEvents, 11},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := newRecordingRegistry()
			tracer := topic(topicAddCounterConfig{
				recordingConfig: recordingConfig{registry: registry, details: test.details},
				capture:         &topicCounterCapture{},
			})
			for _, listener := range []bool{false, true} {
				count := 1
				if listener {
					count = 10
				}
				tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{Listener: listener, MessagesCount: count})
				tracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{Listener: listener, MessagesCount: count})
				tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{Listener: listener, Bytes: count})
				tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{Listener: listener, MessagesDelta: count})
				tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
					Listener: listener, BytesDelta: count,
				})
				tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{Listener: listener, MessagesCount: count})
				tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{Listener: listener, MessagesCount: count})
				for range count {
					tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{Listener: listener})
				}
			}
			for _, path := range []string{
				"ydb.topic.reader.received.messages", "ydb.topic.reader.delivered.messages",
				"ydb.topic.reader.received.bytes", "ydb.topic.reader.local_buffer.messages",
				"ydb.topic.reader.credit_balance_bytes", "ydb.topic.reader.commit.queued",
				"ydb.topic.reader.commit.acknowledged", "ydb.topic.reader.session.errors",
			} {
				require.Equal(t, test.want, registry.value(path, nil), path)
			}
		})
	}
}

type topicMetricDescriptor struct {
	name       string
	legacyName string
	unit       string
	kind       string
	labels     []string
}

type strictTopicConfig struct {
	recordingConfig

	t testing.TB
}

func (c strictTopicConfig) WithSystem(system string) Config {
	scoped := c.recordingConfig.WithSystem(system).(recordingConfig)

	return strictTopicConfig{recordingConfig: scoped, t: c.t}
}

func (c strictTopicConfig) CounterVec(_ string, labelNames ...string) CounterVec {
	return strictCounterVec{t: c.t, labelNames: labelNameSet(labelNames)}
}

func (c strictTopicConfig) GaugeVec(_ string, labelNames ...string) GaugeVec {
	return strictGaugeVec{t: c.t, labelNames: labelNameSet(labelNames)}
}

func labelNameSet(labelNames []string) map[string]struct{} {
	set := make(map[string]struct{}, len(labelNames))
	for _, name := range labelNames {
		set[name] = struct{}{}
	}

	return set
}

type strictCounterVec struct {
	t          testing.TB
	labelNames map[string]struct{}
}

func (v strictCounterVec) With(labels map[string]string) Counter {
	v.t.Helper()
	require.Equal(v.t, v.labelNames, labelNameSetFromValues(labels))

	return strictCounter{}
}

type strictCounter struct{}

func (strictCounter) Inc() {}

type strictGaugeVec struct {
	t          testing.TB
	labelNames map[string]struct{}
}

func (v strictGaugeVec) With(labels map[string]string) Gauge {
	v.t.Helper()
	require.Equal(v.t, v.labelNames, labelNameSetFromValues(labels))

	return strictGauge{}
}

type strictGauge struct{}

func (strictGauge) Add(float64) {}

func (strictGauge) Set(float64) {}

func labelNameSetFromValues(labels map[string]string) map[string]struct{} {
	set := make(map[string]struct{}, len(labels))
	for name := range labels {
		set[name] = struct{}{}
	}

	return set
}

type topicCounterCapture struct {
	units map[string]string
	adds  map[string][]int64
}

type topicAddCounterConfig struct {
	recordingConfig

	capture *topicCounterCapture
}

func (c topicAddCounterConfig) WithSystem(system string) Config {
	scoped := c.recordingConfig.WithSystem(system).(recordingConfig)

	return topicAddCounterConfig{recordingConfig: scoped, capture: c.capture}
}

func (c topicAddCounterConfig) CounterVecWithDescriptor(name, unit string, labels ...string) CounterVec {
	if c.capture.units == nil {
		c.capture.units = make(map[string]string)
	}
	c.capture.units[name] = unit
	c.registry.register(name, "counter", labels)

	return topicAddCounterVec{registry: c.registry, path: name, capture: c.capture}
}

func (c topicAddCounterConfig) GaugeVecWithDescriptor(name, unit string, labels ...string) GaugeVec {
	if c.capture.units == nil {
		c.capture.units = make(map[string]string)
	}
	c.capture.units[name] = unit
	c.registry.register(name, "gauge", labels)

	return recordingGaugeVec{registry: c.registry, path: name}
}

type topicAddCounterVec struct {
	registry *recordingRegistry
	path     string

	capture *topicCounterCapture
}

func (v topicAddCounterVec) With(labels map[string]string) Counter {
	return topicAddCounter{registry: v.registry, path: v.path, labels: labels, capture: v.capture}
}

type topicAddCounter struct {
	registry *recordingRegistry
	path     string
	labels   map[string]string

	capture *topicCounterCapture
}

func (topicAddCounter) Inc() {
	panic("topic Add counter unexpectedly used Inc")
}

func (c topicAddCounter) Add(delta int64) {
	if c.capture.adds == nil {
		c.capture.adds = make(map[string][]int64)
	}
	c.capture.adds[c.path] = append(c.capture.adds[c.path], delta)
	c.registry.add(c.path, c.labels, float64(delta))
}

type topicGaugeCapture struct {
	adds map[string][]float64
	sets map[string]int
}

type topicTrackingGaugeConfig struct {
	recordingConfig

	capture *topicGaugeCapture
}

func (c topicTrackingGaugeConfig) WithSystem(system string) Config {
	scoped := c.recordingConfig.WithSystem(system).(recordingConfig)

	return topicTrackingGaugeConfig{recordingConfig: scoped, capture: c.capture}
}

func (c topicTrackingGaugeConfig) GaugeVec(name string, labels ...string) GaugeVec {
	path := c.path(name)
	c.registry.register(path, "gauge", labels)

	return topicTrackingGaugeVec{registry: c.registry, path: path, capture: c.capture}
}

type topicTrackingGaugeVec struct {
	registry *recordingRegistry
	path     string

	capture *topicGaugeCapture
}

func (v topicTrackingGaugeVec) With(labels map[string]string) Gauge {
	return topicTrackingGauge{registry: v.registry, path: v.path, labels: labels, capture: v.capture}
}

type topicTrackingGauge struct {
	registry *recordingRegistry
	path     string
	labels   map[string]string

	capture *topicGaugeCapture
}

func (g topicTrackingGauge) Add(delta float64) {
	if g.capture.adds == nil {
		g.capture.adds = make(map[string][]float64)
	}
	g.capture.adds[g.path] = append(g.capture.adds[g.path], delta)
	g.registry.add(g.path, g.labels, delta)
}

func (g topicTrackingGauge) Set(value float64) {
	if g.capture.sets == nil {
		g.capture.sets = make(map[string]int)
	}
	g.capture.sets[g.path]++
	g.registry.set(g.path, g.labels, value)
}
