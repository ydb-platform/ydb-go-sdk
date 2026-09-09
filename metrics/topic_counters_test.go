package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicReaderMetricDescriptorsAndLegacyNames(t *testing.T) {
	descriptorRegistry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	topic(topicFloatCounterConfig{
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
	t.Run("message events", func(t *testing.T) {
		registry := newRecordingRegistry()
		tracer := topic(recordingConfig{registry: registry, details: trace.TopicReaderMessageEvents})

		require.NotNil(t, tracer.OnReaderMessagesReceived)
		require.NotNil(t, tracer.OnReaderMessagesDelivered)
		require.NotNil(t, tracer.OnReaderReceivedBytes)
		require.NotNil(t, tracer.OnReaderLocalBufferChanged)
		require.NotNil(t, tracer.OnReaderCreditBalanceChanged)
		require.Nil(t, tracer.OnReaderCommitQueued)
		require.Nil(t, tracer.OnReaderCommitAcknowledged)
		require.Nil(t, tracer.OnReaderSessionError)

		readerName := readerNamePointer("reader")
		tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
			Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
			ReaderName: readerName, MessagesCount: 3,
		})
		tracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{
			Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
			ReaderName: readerName, MessagesCount: 2,
		})
		tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
			Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerName, Bytes: 11,
		})
		tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
			Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
			ReaderName: readerName, MessagesDelta: 4,
		})
		tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
			Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerName, BytesDelta: -7,
		})

		messageLabels := map[string]string{
			"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
		}
		require.Equal(t, float64(3), registry.value("topic.reader.received.messages", messageLabels))
		require.Equal(t, float64(2), registry.value("topic.reader.delivered.messages", messageLabels))
		require.Equal(t, float64(4), registry.value("topic.reader.local_buffer.messages", messageLabels))
		require.Zero(t, registry.value("topic.reader.received.bytes", map[string]string{
			"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
		}))
		require.Equal(t, float64(-7), registry.value("topic.reader.credit_balance_bytes", map[string]string{
			"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
		}))
		require.Zero(t, registry.value("topic.reader.commit.queued", messageLabels))
		require.Zero(t, registry.value("topic.reader.commit.acknowledged", messageLabels))
		require.Zero(t, registry.value("topic.reader.session.errors", map[string]string{
			"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
			"retry_decision": "retry", "status_code": "UNAVAILABLE", "error.type": "transport_error",
		}))
	})

	t.Run("stream events", func(t *testing.T) {
		registry := newRecordingRegistry()
		tracer := topic(recordingConfig{registry: registry, details: trace.TopicReaderStreamEvents})

		require.Nil(t, tracer.OnReaderMessagesReceived)
		require.Nil(t, tracer.OnReaderMessagesDelivered)
		require.Nil(t, tracer.OnReaderReceivedBytes)
		require.Nil(t, tracer.OnReaderLocalBufferChanged)
		require.Nil(t, tracer.OnReaderCreditBalanceChanged)
		require.NotNil(t, tracer.OnReaderCommitQueued)
		require.NotNil(t, tracer.OnReaderCommitAcknowledged)
		require.NotNil(t, tracer.OnReaderSessionError)

		readerName := readerNamePointer("reader")
		tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
			Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
			ReaderName: readerName, MessagesCount: 9,
		})
		tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{
			Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
			ReaderName: readerName, MessagesCount: 8,
		})
		tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
			Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerName,
			RetryDecision: "retry", StatusCode: "UNAVAILABLE", ErrorType: "transport_error",
		})

		messageLabels := map[string]string{
			"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
		}
		require.Zero(t, registry.value("topic.reader.received.messages", messageLabels))
		require.Zero(t, registry.value("topic.reader.delivered.messages", messageLabels))
		require.Zero(t, registry.value("topic.reader.local_buffer.messages", messageLabels))
		require.Zero(t, registry.value("topic.reader.received.bytes", map[string]string{
			"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
		}))
		require.Zero(t, registry.value("topic.reader.credit_balance_bytes", map[string]string{
			"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
		}))
		require.Equal(t, float64(9), registry.value("topic.reader.commit.queued", messageLabels))
		require.Equal(t, float64(8), registry.value("topic.reader.commit.acknowledged", messageLabels))
		require.Equal(t, float64(1), registry.value("topic.reader.session.errors", map[string]string{
			"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
			"retry_decision": "retry", "status_code": "UNAVAILABLE", "error.type": "transport_error",
		}))
	})
}

func TestTopicReaderCountersIgnoreNonPositiveDeltas(t *testing.T) {
	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	tracer := topic(topicFloatCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.DetailsAll},
		capture:         capture,
	})
	readerName := readerNamePointer("reader")

	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerName, MessagesCount: 4,
	})
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{MessagesCount: 0})
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{MessagesCount: -1})
	tracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerName, MessagesCount: 3,
	})
	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerName, Bytes: 12,
	})
	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{Bytes: 0})
	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{Bytes: -1})
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerName, MessagesCount: 2,
	})
	tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerName, MessagesCount: 1,
	})
	tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerName,
		RetryDecision: "retry", StatusCode: "UNAVAILABLE", ErrorType: "transport_error",
	})

	require.Equal(t, map[string][]float64{
		"ydb.topic.reader.received.messages":   {4},
		"ydb.topic.reader.delivered.messages":  {3},
		"ydb.topic.reader.received.bytes":      {12},
		"ydb.topic.reader.commit.queued":       {2},
		"ydb.topic.reader.commit.acknowledged": {1},
		"ydb.topic.reader.session.errors":      {1},
	}, capture.floatAdds)
	require.Empty(t, capture.incs)

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
	require.Equal(t, float64(4), registry.value("ydb.topic.reader.received.messages", messageLabels))
	require.Equal(t, float64(3), registry.value("ydb.topic.reader.delivered.messages", messageLabels))
	require.Equal(t, float64(2), registry.value("ydb.topic.reader.commit.queued", messageLabels))
	require.Equal(t, float64(1), registry.value("ydb.topic.reader.commit.acknowledged", messageLabels))
	require.Equal(t, float64(12), registry.value("ydb.topic.reader.received.bytes", streamLabels))
	require.Equal(t, float64(1), registry.value("ydb.topic.reader.session.errors", errorLabels))
}

func TestTopicReaderCountersFallBackToIncExceptBytes(t *testing.T) {
	fallbackRegistry := newRecordingRegistry()
	fallbackCapture := &topicCounterCapture{}
	fallbackTracer := topic(topicFallbackCounterConfig{
		recordingConfig: recordingConfig{registry: fallbackRegistry, details: trace.TopicReaderMessageEvents},
		capture:         fallbackCapture,
	})
	fallbackTracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerNamePointer("reader"), MessagesCount: 3,
	})
	fallbackTracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer",
		ReaderName: readerNamePointer("reader"), Bytes: 1024,
	})

	require.Empty(t, fallbackCapture.floatAdds)
	require.Equal(t, map[string]int{"topic.reader.received.messages": 3}, fallbackCapture.incs)
	require.Equal(t, float64(3), fallbackRegistry.value("topic.reader.received.messages", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Zero(t, fallbackRegistry.value("topic.reader.received.bytes", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
	}))
}

func TestTopicReaderCountersUseFloatAddForLargeDeltas(t *testing.T) {
	const largeDelta = 1 << 20

	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	tracer := topic(topicFloatCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.DetailsAll},
		capture:         capture,
	})

	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), Bytes: largeDelta,
	})
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		Endpoint:      "node",
		Database:      "/db",
		Topic:         "/topic",
		Consumer:      "consumer",
		ReaderName:    readerNamePointer("reader"),
		MessagesCount: largeDelta,
	})
	tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{
		Endpoint:      "node",
		Database:      "/db",
		Topic:         "/topic",
		Consumer:      "consumer",
		ReaderName:    readerNamePointer("reader"),
		MessagesCount: largeDelta,
	})

	require.Equal(t, map[string][]float64{
		"ydb.topic.reader.received.bytes":      {float64(largeDelta)},
		"ydb.topic.reader.commit.queued":       {float64(largeDelta)},
		"ydb.topic.reader.commit.acknowledged": {float64(largeDelta)},
	}, capture.floatAdds)
	require.Empty(t, capture.incs)
	require.Equal(t, float64(largeDelta), registry.value("ydb.topic.reader.received.bytes", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Equal(t, float64(largeDelta), registry.value("ydb.topic.reader.commit.queued", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Equal(t, float64(largeDelta), registry.value("ydb.topic.reader.commit.acknowledged", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}))
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
		ReaderName: readerNamePointer("reader"), MessagesDelta: 5,
	})
	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerNamePointer("reader"), MessagesDelta: -2,
	})
	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{MessagesDelta: 0})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), BytesDelta: 100,
	})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), BytesDelta: -40,
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
		readerName *string
	}{
		{name: "without consumer or reader name"},
		{name: "without consumer with reader name", readerName: readerNamePointer("generated-reader")},
		{name: "with consumer and reader name", consumer: "consumer", readerName: readerNamePointer("reader")},
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
			tracer := topic(topicFloatCounterConfig{
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
	units     map[string]string
	floatAdds map[string][]float64
	incs      map[string]int
}

type topicFloatCounterConfig struct {
	recordingConfig

	capture *topicCounterCapture
}

func (c topicFloatCounterConfig) WithSystem(system string) Config {
	scoped := c.recordingConfig.WithSystem(system).(recordingConfig)

	return topicFloatCounterConfig{recordingConfig: scoped, capture: c.capture}
}

func (c topicFloatCounterConfig) CounterVecWithDescriptor(name, unit string, labels ...string) CounterVec {
	if c.capture.units == nil {
		c.capture.units = make(map[string]string)
	}
	c.capture.units[name] = unit
	c.registry.register(name, "counter", labels)

	return topicFloatCounterVec{registry: c.registry, path: name, capture: c.capture}
}

func (c topicFloatCounterConfig) GaugeVecWithDescriptor(name, unit string, labels ...string) GaugeVec {
	if c.capture.units == nil {
		c.capture.units = make(map[string]string)
	}
	c.capture.units[name] = unit
	c.registry.register(name, "gauge", labels)

	return recordingGaugeVec{registry: c.registry, path: name}
}

type topicFloatCounterVec struct {
	registry *recordingRegistry
	path     string

	capture *topicCounterCapture
}

func (v topicFloatCounterVec) With(labels map[string]string) Counter {
	return topicFloatCounter{registry: v.registry, path: v.path, labels: labels, capture: v.capture}
}

type topicFloatCounter struct {
	registry *recordingRegistry
	path     string
	labels   map[string]string

	capture *topicCounterCapture
}

func (topicFloatCounter) Inc() {
	panic("topic float counter unexpectedly used Inc")
}

func (c topicFloatCounter) Add(delta float64) {
	if c.capture.floatAdds == nil {
		c.capture.floatAdds = make(map[string][]float64)
	}
	c.capture.floatAdds[c.path] = append(c.capture.floatAdds[c.path], delta)
	c.registry.add(c.path, c.labels, delta)
}

type topicFallbackCounterConfig struct {
	recordingConfig

	capture *topicCounterCapture
}

func (c topicFallbackCounterConfig) WithSystem(system string) Config {
	scoped := c.recordingConfig.WithSystem(system).(recordingConfig)

	return topicFallbackCounterConfig{recordingConfig: scoped, capture: c.capture}
}

func (c topicFallbackCounterConfig) CounterVec(name string, labels ...string) CounterVec {
	path := c.path(name)
	c.registry.register(path, "counter", labels)

	return topicFallbackCounterVec{registry: c.registry, path: path, capture: c.capture}
}

type topicFallbackCounterVec struct {
	registry *recordingRegistry
	path     string

	capture *topicCounterCapture
}

func (v topicFallbackCounterVec) With(labels map[string]string) Counter {
	return topicFallbackCounter{registry: v.registry, path: v.path, labels: labels, capture: v.capture}
}

type topicFallbackCounter struct {
	registry *recordingRegistry
	path     string
	labels   map[string]string

	capture *topicCounterCapture
}

func (c topicFallbackCounter) Inc() {
	if c.capture.incs == nil {
		c.capture.incs = make(map[string]int)
	}
	c.capture.incs[c.path]++
	c.registry.add(c.path, c.labels, 1)
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

func readerNamePointer(name string) *string {
	return &name
}
