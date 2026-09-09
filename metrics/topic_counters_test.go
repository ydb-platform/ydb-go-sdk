package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicReaderMetricDescriptorsUseCanonicalMetadata(t *testing.T) {
	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}

	topic(topicBatchCounterConfig{
		recordingConfig: recordingConfig{
			registry: registry,
			system:   "custom",
			details:  trace.DetailsAll,
		},
		capture: capture,
	})

	expected := []topicMetricDescriptor{
		{
			name:   "ydb.topic.reader.received.messages",
			unit:   "{message}",
			kind:   "counter",
			labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		{
			name:   "ydb.topic.reader.delivered.messages",
			unit:   "{message}",
			kind:   "counter",
			labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		{
			name:   "ydb.topic.reader.received.bytes",
			unit:   "By",
			kind:   "counter",
			labels: []string{"endpoint", "database", "consumer", "reader.name"},
		},
		{
			name:   "ydb.topic.reader.session.errors",
			unit:   "{error}",
			kind:   "counter",
			labels: []string{"endpoint", "database", "consumer", "reader.name", "retry_decision", "status_code", "error.type"},
		},
		{
			name:   "ydb.topic.reader.commit.queued",
			unit:   "{message}",
			kind:   "counter",
			labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		{
			name:   "ydb.topic.reader.commit.acknowledged",
			unit:   "{message}",
			kind:   "counter",
			labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		{
			name:   "ydb.topic.reader.local_buffer.messages",
			unit:   "{message}",
			kind:   "gauge",
			labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		{
			name:   "ydb.topic.reader.credit_balance_bytes",
			unit:   "By",
			kind:   "gauge",
			labels: []string{"endpoint", "database", "consumer", "reader.name"},
		},
	}

	for _, metric := range expected {
		require.Equal(t, metric.unit, capture.units[metric.name], metric.name)
		require.Equal(t, metric.kind, registry.kinds[metric.name], metric.name)
		require.Equal(t, metric.labels, registry.labelNames[metric.name], metric.name)
	}
	require.Len(t, capture.units, len(expected))
}

func TestTopicReaderLegacyMetricsKeepDottedScopeAndCreditLeaf(t *testing.T) {
	registry := newRecordingRegistry()

	topic(recordingConfig{
		registry: registry,
		system:   "custom",
		details:  trace.DetailsAll,
	})

	expected := map[string]struct {
		kind   string
		labels []string
	}{
		"custom.topic.reader.received.messages": {
			kind: "counter", labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		"custom.topic.reader.delivered.messages": {
			kind: "counter", labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		"custom.topic.reader.received.bytes": {
			kind: "counter", labels: []string{"endpoint", "database", "consumer", "reader.name"},
		},
		"custom.topic.reader.session.errors": {
			kind: "counter",
			labels: []string{
				"endpoint", "database", "consumer", "reader.name", "retry_decision", "status_code", "error.type",
			},
		},
		"custom.topic.reader.commit.queued": {
			kind: "counter", labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		"custom.topic.reader.commit.acknowledged": {
			kind: "counter", labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		"custom.topic.reader.local_buffer.messages": {
			kind: "gauge", labels: []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		},
		"custom.topic.reader.credit_balance_bytes": {
			kind: "gauge", labels: []string{"endpoint", "database", "consumer", "reader.name"},
		},
	}

	for name, metric := range expected {
		require.Equal(t, metric.kind, registry.kinds[name], name)
		require.Equal(t, metric.labels, registry.labelNames[name], name)
	}
	require.NotContains(t, registry.kinds, "custom.topic.reader.credit_balance.bytes")
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

		tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
			Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
			ReaderName: readerNamePointer("reader"), MessagesCount: 3,
		})
		tracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{
			Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
			ReaderName: readerNamePointer("reader"), MessagesCount: 2,
		})
		tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
			Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), Bytes: 11,
		})
		tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
			Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
			ReaderName: readerNamePointer("reader"), MessagesDelta: 4,
		})
		tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
			Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), BytesDelta: -7,
		})

		if tracer.OnReaderCommitQueued != nil {
			tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
				Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
				ReaderName: readerNamePointer("reader"), MessagesCount: 9,
			})
		}
		if tracer.OnReaderCommitAcknowledged != nil {
			tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{
				Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
				ReaderName: readerNamePointer("reader"), MessagesCount: 8,
			})
		}
		if tracer.OnReaderSessionError != nil {
			tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
				Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"),
				RetryDecision: "retry", StatusCode: "UNAVAILABLE", ErrorType: "transport_error",
			})
		}

		messageLabels := map[string]string{
			"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
		}
		require.Equal(t, float64(3), registry.value("topic.reader.received.messages", messageLabels))
		require.Equal(t, float64(2), registry.value("topic.reader.delivered.messages", messageLabels))
		require.Equal(t, float64(4), registry.value("topic.reader.local_buffer.messages", messageLabels))
		require.Equal(t, float64(11), registry.value("topic.reader.received.bytes", map[string]string{
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

		require.NotNil(t, tracer.OnReaderCommitQueued)
		require.NotNil(t, tracer.OnReaderCommitAcknowledged)
		require.NotNil(t, tracer.OnReaderSessionError)

		if tracer.OnReaderMessagesReceived != nil {
			tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
				Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
				ReaderName: readerNamePointer("reader"), MessagesCount: 3,
			})
		}
		if tracer.OnReaderMessagesDelivered != nil {
			tracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{
				Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
				ReaderName: readerNamePointer("reader"), MessagesCount: 2,
			})
		}
		if tracer.OnReaderReceivedBytes != nil {
			tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
				Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), Bytes: 11,
			})
		}
		if tracer.OnReaderLocalBufferChanged != nil {
			tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
				Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
				ReaderName: readerNamePointer("reader"), MessagesDelta: 4,
			})
		}
		if tracer.OnReaderCreditBalanceChanged != nil {
			tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
				Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), BytesDelta: -7,
			})
		}

		tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
			Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
			ReaderName: readerNamePointer("reader"), MessagesCount: 9,
		})
		tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{
			Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
			ReaderName: readerNamePointer("reader"), MessagesCount: 8,
		})
		tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
			Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"),
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

func TestTopicReaderCountersUseBatchAddAndIgnoreNonPositive(t *testing.T) {
	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	tracer := topic(topicBatchCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.DetailsAll},
		capture:         capture,
	})

	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerNamePointer("reader"), MessagesCount: 4,
	})
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{MessagesCount: 0})
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{MessagesCount: -1})
	tracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerNamePointer("reader"), MessagesCount: 3,
	})
	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), Bytes: 12,
	})
	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{Bytes: 0})
	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{Bytes: -2})
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerNamePointer("reader"), MessagesCount: 2,
	})
	tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerNamePointer("reader"), MessagesCount: 1,
	})
	tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"),
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
	require.Empty(t, capture.incs)

	messageLabels := map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}
	require.Equal(t, float64(4), registry.value("ydb.topic.reader.received.messages", messageLabels))
	require.Equal(t, float64(3), registry.value("ydb.topic.reader.delivered.messages", messageLabels))
	require.Equal(t, float64(2), registry.value("ydb.topic.reader.commit.queued", messageLabels))
	require.Equal(t, float64(1), registry.value("ydb.topic.reader.commit.acknowledged", messageLabels))
	require.Equal(t, float64(12), registry.value("ydb.topic.reader.received.bytes", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Equal(t, float64(1), registry.value("ydb.topic.reader.session.errors", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
		"retry_decision": "retry", "status_code": "UNAVAILABLE", "error.type": "transport_error",
	}))

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

	require.Empty(t, fallbackCapture.adds)
	require.Equal(t, map[string]int{"topic.reader.received.messages": 3}, fallbackCapture.incs)
	require.Equal(t, float64(3), fallbackRegistry.value("topic.reader.received.messages", messageLabels))
}

func TestTopicReaderCountersUseFloatAddForLargeDeltas(t *testing.T) {
	const largeDelta = 1 << 20

	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	tracer := topic(topicBatchCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.DetailsAll},
		capture:         capture,
		floatAdd:        true,
	})

	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), Bytes: largeDelta,
	})
	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{Bytes: 0})
	tracer.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{Bytes: -1})
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		Endpoint:      "node",
		Database:      "/db",
		Topic:         "/topic",
		Consumer:      "consumer",
		ReaderName:    readerNamePointer("reader"),
		MessagesCount: largeDelta,
	})
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{MessagesCount: 0})
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{MessagesCount: -1})
	tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{
		Endpoint:      "node",
		Database:      "/db",
		Topic:         "/topic",
		Consumer:      "consumer",
		ReaderName:    readerNamePointer("reader"),
		MessagesCount: largeDelta,
	})
	tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{MessagesCount: 0})
	tracer.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{MessagesCount: -1})

	require.Equal(t, map[string][]float64{
		topicReaderReceivedBytesName:      {float64(largeDelta)},
		topicReaderCommitQueuedName:       {float64(largeDelta)},
		topicReaderCommitAcknowledgedName: {float64(largeDelta)},
	}, capture.floatAdds)
	require.Empty(t, capture.incs)
	require.Equal(t, float64(largeDelta), registry.value(topicReaderReceivedBytesName, map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Equal(t, float64(largeDelta), registry.value(topicReaderCommitQueuedName, map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Equal(t, float64(largeDelta), registry.value(topicReaderCommitAcknowledgedName, map[string]string{
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
	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
		Endpoint: "node", Database: "/db", Topic: "/topic", Consumer: "consumer",
		ReaderName: readerNamePointer("reader"), MessagesDelta: -3,
	})
	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{MessagesDelta: 0})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), BytesDelta: 100,
	})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), BytesDelta: -40,
	})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: readerNamePointer("reader"), BytesDelta: -60,
	})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: 0})

	require.Equal(t, []float64{5, -2, -3}, capture.adds["topic.reader.local_buffer.messages"])
	require.Equal(t, []float64{100, -40, -60}, capture.adds["topic.reader.credit_balance_bytes"])
	require.Empty(t, capture.sets)
	require.Zero(t, registry.value("topic.reader.local_buffer.messages", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader.name": "reader",
	}))
	require.Zero(t, registry.value("topic.reader.credit_balance_bytes", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "reader",
	}))
}

type topicMetricDescriptor struct {
	name   string
	unit   string
	kind   string
	labels []string
}

type topicCounterCapture struct {
	units     map[string]string
	adds      map[string][]int64
	floatAdds map[string][]float64
	incs      map[string]int
}

type topicBatchCounterConfig struct {
	recordingConfig

	capture  *topicCounterCapture
	floatAdd bool
}

func (c topicBatchCounterConfig) WithSystem(system string) Config {
	scoped := c.recordingConfig.WithSystem(system).(recordingConfig)

	return topicBatchCounterConfig{recordingConfig: scoped, capture: c.capture, floatAdd: c.floatAdd}
}

func (c topicBatchCounterConfig) CounterVecWithDescriptor(name, unit string, labels ...string) CounterVec {
	if c.capture.units == nil {
		c.capture.units = make(map[string]string)
	}
	c.capture.units[name] = unit
	c.registry.register(name, "counter", labels)
	if c.floatAdd {
		return topicFloatCounterVec{registry: c.registry, path: name, capture: c.capture}
	}

	return topicBatchCounterVec{registry: c.registry, path: name, capture: c.capture}
}

func (c topicBatchCounterConfig) GaugeVecWithDescriptor(name, unit string, labels ...string) GaugeVec {
	if c.capture.units == nil {
		c.capture.units = make(map[string]string)
	}
	c.capture.units[name] = unit
	c.registry.register(name, "gauge", labels)

	return recordingGaugeVec{registry: c.registry, path: name}
}

type topicBatchCounterVec struct {
	registry *recordingRegistry
	path     string

	capture *topicCounterCapture
}

func (v topicBatchCounterVec) With(labels map[string]string) Counter {
	return topicBatchCounter{registry: v.registry, path: v.path, labels: labels, capture: v.capture}
}

type topicBatchCounter struct {
	registry *recordingRegistry
	path     string
	labels   map[string]string

	capture *topicCounterCapture
}

func (c topicBatchCounter) Inc() {
	if c.capture.incs == nil {
		c.capture.incs = make(map[string]int)
	}
	c.capture.incs[c.path]++
	c.registry.add(c.path, c.labels, 1)
}

func (c topicBatchCounter) Add(delta int64) {
	if c.capture.adds == nil {
		c.capture.adds = make(map[string][]int64)
	}
	c.capture.adds[c.path] = append(c.capture.adds[c.path], delta)
	c.registry.add(c.path, c.labels, float64(delta))
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
			tracer := topic(recordingConfig{registry: registry, details: test.details})
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
			for _, name := range []string{
				"received.messages", "delivered.messages", "received.bytes", "local_buffer.messages",
				"credit_balance_bytes", "commit.queued", "commit.acknowledged", "session.errors",
			} {
				require.Equal(t, test.want, registry.value("topic.reader."+name, nil), name)
			}
		})
	}
}

func TestTopicMetricOptionalLabelsMatchDotNet(t *testing.T) {
	require.Equal(t, map[string]string{"endpoint": "node", "database": "/db"}, streamLabels("node", "/db", "", nil))
	require.Equal(t, map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "reader.name": "",
	}, messageLabels("node", "/db", "/topic", "", readerNamePointer("")))
	require.Equal(t, map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader.name": "named",
	}, streamLabels("node", "/db", "consumer", readerNamePointer("named")))
}
