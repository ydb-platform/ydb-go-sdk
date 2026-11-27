package metrics

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	ydblog "github.com/ydb-platform/ydb-go-sdk/v3/log"
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
			unit: "{message}", kind: "gauge", labels: []string{"endpoint", "database", "consumer", "reader.name"},
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
		legacyLabels := strings.NewReplacer("reader.name", "reader_name", "error.type", "error_type").
			Replace(strings.Join(metric.labels, ","))
		require.Equal(t, legacyLabels, strings.Join(legacyRegistry.labelNames[metric.legacyName], ","), metric.legacyName)
	}
	require.Len(t, capture.units, len(expected))
	require.NotContains(t, legacyRegistry.kinds, "custom.topic.reader.credit_balance.bytes")
}

func TestDisabledTopicMetricsDoNotEnableTracking(t *testing.T) {
	tracer := topic(recordingConfig{
		registry: newRecordingRegistry(), details: trace.TopicReaderCustomerEvents,
	})
	require.Nil(t, tracer.Reader)
	require.Nil(t, tracer.Listener)
}

func TestTopicReaderMetricsHonorDetailsGroups(t *testing.T) {
	messageEvents := topic(recordingConfig{
		registry: newRecordingRegistry(), details: trace.TopicReaderMessageEvents,
	}).Reader
	require.NotNil(t, messageEvents.OnReaderMessagesReceived)
	require.NotNil(t, messageEvents.OnReaderMessagesDelivered)
	require.NotNil(t, messageEvents.OnReaderReceiveDataResponse)
	require.Nil(t, messageEvents.OnListenerReceiveMessage)
	require.NotNil(t, messageEvents.OnReaderLocalBufferChanged)
	require.NotNil(t, messageEvents.OnReaderCreditBalanceChanged)
	require.Nil(t, messageEvents.OnReaderCommitQueued)
	require.Nil(t, messageEvents.OnReaderCommittedNotify)
	require.Nil(t, messageEvents.OnReaderSessionError)

	streamEvents := topic(recordingConfig{
		registry: newRecordingRegistry(), details: trace.TopicReaderStreamEvents,
	}).Reader
	require.Nil(t, streamEvents.OnReaderMessagesReceived)
	require.Nil(t, streamEvents.OnReaderMessagesDelivered)
	require.Nil(t, streamEvents.OnReaderReceiveDataResponse)
	require.Nil(t, streamEvents.OnListenerReceiveMessage)
	require.Nil(t, streamEvents.OnReaderLocalBufferChanged)
	require.Nil(t, streamEvents.OnReaderCreditBalanceChanged)
	require.NotNil(t, streamEvents.OnReaderCommitQueued)
	require.Nil(t, streamEvents.OnListenerCommitQueued)
	require.NotNil(t, streamEvents.OnReaderCommittedNotify)
	require.NotNil(t, streamEvents.OnReaderSessionError)

	listenerEvents := topic(recordingConfig{
		registry: newRecordingRegistry(), details: trace.TopicListenerStreamEvents,
	}).Listener
	require.Nil(t, listenerEvents.OnReaderMessagesReceived)
	require.NotNil(t, listenerEvents.OnListenerMessagesReceived)
	require.Nil(t, listenerEvents.OnReaderMessagesDelivered)
	require.NotNil(t, listenerEvents.OnListenerMessagesDelivered)
	require.Nil(t, listenerEvents.OnReaderCreditBalanceChanged)
	require.NotNil(t, listenerEvents.OnListenerCreditBalanceChanged)
	require.Nil(t, listenerEvents.OnReaderLocalBufferChanged)
	require.NotNil(t, listenerEvents.OnListenerLocalBufferChanged)
	require.Nil(t, listenerEvents.OnReaderReceiveDataResponse)
	require.NotNil(t, listenerEvents.OnListenerReceiveMessage)
	require.Nil(t, listenerEvents.OnReaderCommitQueued)
	require.NotNil(t, listenerEvents.OnListenerCommitQueued)
	require.NotNil(t, listenerEvents.OnReaderCommittedNotify)
}

func TestTopicReaderCountersIgnoreNonPositiveDeltas(t *testing.T) {
	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	tracer := topic(topicAddCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.DetailsAll},
		capture:         capture,
	})
	for _, count := range []int{0, -1} {
		tracer.Reader.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{MessagesCount: count})
		tracer.Reader.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{MessagesCount: count})
		tracer.Reader.OnReaderReceiveDataResponse(readerBytesInfo(trace.TopicReaderInfo{}, count))
		tracer.Listener.OnListenerReceiveMessage(listenerBytesInfo(trace.TopicReaderInfo{}, count, nil))
		tracer.Reader.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{MessagesCount: int64(count)})
		tracer.Reader.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{MessagesCount: int64(count)})
	}
	tracer.Listener.OnListenerReceiveMessage(listenerBytesInfo(trace.TopicReaderInfo{}, 10, errors.New("receive failed")))
	require.Empty(t, capture.adds)
	require.Empty(t, registry.values)
}

func TestTopicReaderCountersRecordLabeledValues(t *testing.T) {
	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	messageTracer := topic(topicAddCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.TopicReaderMessageEvents},
		capture:         capture,
	}).Reader
	streamTracer := topic(topicAddCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.TopicReaderStreamEvents},
		capture:         capture,
	}).Reader
	listenerTracer := topic(topicAddCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.TopicListenerStreamEvents},
		capture:         capture,
	}).Listener
	readerInfo := trace.TopicReaderInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: "reader",
	}
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
		ReaderInfo: readerInfo, Topic: "/topic", MessagesCount: 4,
	})
	messageTracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{
		ReaderInfo: readerInfo,
		Topic:      "/topic", MessagesCount: 3,
	})
	messageTracer.OnReaderReceiveDataResponse(readerBytesInfo(readerInfo, 12))
	listenerTracer.OnListenerReceiveMessage(listenerBytesInfo(readerInfo, 8, nil))
	streamTracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		ReaderInfo: readerInfo,
		Topic:      "/topic", MessagesCount: 2,
	})
	streamTracer.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{
		ReaderInfo: readerInfo,
		Topic:      "/topic", MessagesCount: 1,
	})
	streamTracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
		ReaderInfo:    readerInfo,
		RetryDecision: "retry", StatusCode: "UNAVAILABLE", ErrorType: "transport_error",
	})

	require.Equal(t, map[string][]int64{
		"ydb.topic.reader.received.messages":   {4},
		"ydb.topic.reader.delivered.messages":  {3},
		"ydb.topic.reader.received.bytes":      {12, 8},
		"ydb.topic.reader.commit.queued":       {2},
		"ydb.topic.reader.commit.acknowledged": {1},
	}, capture.adds)
	other := trace.TopicReaderInfo{Endpoint: "other", Database: "/other", ReaderName: "second"}
	messageTracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
		ReaderInfo: other, Topic: "/other/topic", MessagesCount: 2,
	})
	require.Equal(t, float64(2), registry.value("ydb.topic.reader.received.messages", map[string]string{
		"endpoint": "other", "database": "/other", "topic": "/other/topic", "reader.name": "second",
	}))
	require.Equal(t, float64(4), registry.value("ydb.topic.reader.received.messages", messageLabels))
	require.Equal(t, float64(3), registry.value("ydb.topic.reader.delivered.messages", messageLabels))
	require.Equal(t, float64(2), registry.value("ydb.topic.reader.commit.queued", messageLabels))
	require.Equal(t, float64(1), registry.value("ydb.topic.reader.commit.acknowledged", messageLabels))
	require.Equal(t, float64(20), registry.value("ydb.topic.reader.received.bytes", streamLabels))
	require.Equal(t, float64(1), registry.value("ydb.topic.reader.session.errors", errorLabels))
}

func TestTopicReaderMetricsComposeWithLogging(t *testing.T) {
	for _, metricsFirst := range []bool{true, false} {
		name := "logging first"
		if metricsFirst {
			name = "metrics first"
		}
		t.Run(name, func(t *testing.T) {
			registry := newRecordingRegistry()
			capture := &topicCounterCapture{}
			config := recordingConfig{registry: registry, details: trace.DetailsAll}
			metricsTrace := topic(topicAddCounterConfig{recordingConfig: config, capture: capture})
			var logs bytes.Buffer
			loggingTrace := ydblog.Topic(
				ydblog.Default(&logs, ydblog.WithMinLevel(ydblog.TRACE)),
				config,
			)

			compose := func(t *trace.Topic) *trace.Topic {
				if metricsFirst {
					return gtrace.Compose(t, &loggingTrace)
				}

				return gtrace.Compose(&loggingTrace, t)
			}
			composed := compose(metricsTrace.Reader)

			readerInfo := trace.TopicReaderInfo{}
			finishReaderReceive := composed.OnReaderReceiveDataResponse(trace.TopicReaderReceiveDataResponseStartInfo{
				ReaderInfo:   readerInfo,
				DataResponse: &rawtopicreader.ReadResponse{BytesSize: 7},
			})
			require.NotNil(t, finishReaderReceive)
			finishReaderReceive(trace.TopicReaderReceiveDataResponseDoneInfo{})
			compose(metricsTrace.Listener).OnListenerReceiveMessage(trace.TopicListenerReceiveMessageInfo{
				ReaderInfo:  readerInfo,
				MessageType: "ReadResponse",
				BytesSize:   11,
			})
			composed.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{MessagesCount: 3})
			composed.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{MessagesCount: 2})

			require.Equal(t, []int64{7, 11}, capture.adds["ydb.topic.reader.received.bytes"])
			require.Equal(t, []int64{2}, capture.adds["ydb.topic.reader.commit.acknowledged"])
			require.Equal(t, 1, strings.Count(
				logs.String(),
				"topic reader data response received and process done",
			))
		})
	}
}

func TestTopicReaderCountersSupportLegacyCounterConfig(t *testing.T) {
	registry := newRecordingRegistry()
	tracer := topic(recordingConfig{registry: registry, system: "custom", details: trace.DetailsAll}).Reader
	readerInfo := trace.TopicReaderInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: "reader",
	}
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
		ReaderInfo: readerInfo,
		Topic:      "/topic", MessagesCount: 3,
	})
	tracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{ReaderInfo: readerInfo, MessagesCount: 1})
	tracer.OnReaderReceiveDataResponse(readerBytesInfo(readerInfo, 1))
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		ReaderInfo: readerInfo,
		Topic:      "/topic", MessagesCount: 1,
	})
	tracer.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{
		ReaderInfo: readerInfo,
		Topic:      "/topic", MessagesCount: 1,
	})
	tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
		ReaderInfo:    readerInfo,
		RetryDecision: "retry", StatusCode: "UNAVAILABLE", ErrorType: "transport_error",
	})

	require.Len(t, registry.values, 1)
	require.Equal(t, float64(0), registry.value("custom.topic.reader.received.messages", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader_name": "reader",
	}))
	require.Equal(t, float64(0), registry.value("custom.topic.reader.received.bytes", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader_name": "reader",
	}))
	require.Equal(t, float64(0), registry.value("custom.topic.reader.commit.queued", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader_name": "reader",
	}))
	require.Equal(t, float64(0), registry.value("custom.topic.reader.commit.acknowledged", map[string]string{
		"endpoint": "node", "database": "/db", "topic": "/topic", "consumer": "consumer", "reader_name": "reader",
	}))
	require.Equal(t, float64(1), registry.value("custom.topic.reader.session.errors", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader_name": "reader",
		"retry_decision": "retry", "status_code": "UNAVAILABLE", "error_type": "transport_error",
	}))
}

func TestTopicReaderCountersUseInt64AddForLargeDeltas(t *testing.T) {
	largeDelta := int(^uint(0) >> 1)

	registry := newRecordingRegistry()
	capture := &topicCounterCapture{}
	tracer := topic(topicAddCounterConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.DetailsAll},
		capture:         capture,
	}).Reader
	readerInfo := trace.TopicReaderInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: "reader",
	}

	tracer.OnReaderReceiveDataResponse(readerBytesInfo(readerInfo, largeDelta))
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		ReaderInfo:    readerInfo,
		Topic:         "/topic",
		MessagesCount: math.MaxInt64,
	})
	tracer.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{
		ReaderInfo:    readerInfo,
		Topic:         "/topic",
		MessagesCount: math.MaxInt64,
	})

	require.Equal(t, map[string][]int64{
		"ydb.topic.reader.received.bytes":      {int64(largeDelta)},
		"ydb.topic.reader.commit.queued":       {math.MaxInt64},
		"ydb.topic.reader.commit.acknowledged": {math.MaxInt64},
	}, capture.adds)
}

func TestTopicReaderGaugesApplyDeltasWithoutSet(t *testing.T) {
	registry := newRecordingRegistry()
	capture := &topicGaugeCapture{}
	tracer := topic(topicTrackingGaugeConfig{
		recordingConfig: recordingConfig{registry: registry, details: trace.TopicReaderMessageEvents},
		capture:         capture,
	}).Reader
	readerInfo := trace.TopicReaderInfo{
		Endpoint: "node", Database: "/db", Consumer: "consumer", ReaderName: "reader",
	}

	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
		ReaderInfo:    readerInfo,
		MessagesDelta: 5,
	})
	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
		ReaderInfo:    readerInfo,
		MessagesDelta: -2,
	})
	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{MessagesDelta: 0})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
		ReaderInfo: readerInfo,
		BytesDelta: 100,
	})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
		ReaderInfo: readerInfo,
		BytesDelta: -40,
	})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: 0})

	require.Equal(t, []float64{5, -2}, capture.adds["topic.reader.local_buffer.messages"])
	require.Equal(t, []float64{100, -40}, capture.adds["topic.reader.credit_balance_bytes"])
	require.Empty(t, capture.sets)
	require.Equal(t, float64(3), registry.value("topic.reader.local_buffer.messages", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader_name": "reader",
	}))
	require.Equal(t, float64(60), registry.value("topic.reader.credit_balance_bytes", map[string]string{
		"endpoint": "node", "database": "/db", "consumer": "consumer", "reader_name": "reader",
	}))
}

func TestTopicMetricsSeparateReaderAndListenerDetails(t *testing.T) {
	for _, test := range []struct {
		name    string
		details trace.Details
		want    float64
	}{
		{"reader", trace.TopicReaderEvents, 1},
		{"listener", trace.TopicListenerStreamEvents, 10},
		{"both", trace.TopicEvents, 11},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := newRecordingRegistry()
			traces := topic(topicAddCounterConfig{
				recordingConfig: recordingConfig{registry: registry, details: test.details},
				capture:         &topicCounterCapture{},
			})
			for _, listener := range []bool{false, true} {
				tracer, count := traces.Reader, 1
				if listener {
					tracer, count = traces.Listener, 10
				}
				if tracer == nil {
					continue
				}
				readerInfo := trace.TopicReaderInfo{Listener: listener}
				received := tracer.OnReaderMessagesReceived
				delivered := tracer.OnReaderMessagesDelivered
				credit := tracer.OnReaderCreditBalanceChanged
				local := tracer.OnReaderLocalBufferChanged
				if listener {
					received = tracer.OnListenerMessagesReceived
					delivered = tracer.OnListenerMessagesDelivered
					credit = tracer.OnListenerCreditBalanceChanged
					local = tracer.OnListenerLocalBufferChanged
				}
				if received != nil {
					received(trace.TopicReaderMessagesReceivedInfo{
						ReaderInfo: readerInfo, MessagesCount: count,
					})
				}
				if delivered != nil {
					delivered(trace.TopicReaderMessagesDeliveredInfo{
						ReaderInfo: readerInfo, MessagesCount: count,
					})
				}
				if listener && tracer.OnListenerReceiveMessage != nil {
					tracer.OnListenerReceiveMessage(listenerBytesInfo(readerInfo, count, nil))
				} else if !listener && tracer.OnReaderReceiveDataResponse != nil {
					tracer.OnReaderReceiveDataResponse(readerBytesInfo(readerInfo, count))
				}
				if local != nil {
					local(trace.TopicReaderLocalBufferChangedInfo{
						ReaderInfo: readerInfo, MessagesDelta: count,
					})
				}
				if credit != nil {
					credit(trace.TopicReaderCreditBalanceChangedInfo{
						ReaderInfo: readerInfo, BytesDelta: count,
					})
				}
				queued := trace.TopicReaderCommitQueuedInfo{ReaderInfo: readerInfo, MessagesCount: int64(count)}
				if listener && tracer.OnListenerCommitQueued != nil {
					tracer.OnListenerCommitQueued(queued)
				} else if !listener && tracer.OnReaderCommitQueued != nil {
					tracer.OnReaderCommitQueued(queued)
				}
				tracer.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{
					ReaderInfo: readerInfo, MessagesCount: int64(count),
				})
				for range count {
					tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{ReaderInfo: readerInfo})
				}
			}
			for _, path := range []string{
				"ydb.topic.reader.received.messages", "ydb.topic.reader.delivered.messages",
				"ydb.topic.reader.received.bytes", "ydb.topic.reader.local_buffer.messages",
				"ydb.topic.reader.credit_balance_bytes", "ydb.topic.reader.commit.queued",
				"ydb.topic.reader.commit.acknowledged", "ydb.topic.reader.session.errors",
			} {
				labels := make(map[string]string)
				for _, key := range registry.labelNames[path] {
					if key != consumerLabel {
						labels[key] = ""
					}
				}
				require.Equal(t, test.want, registry.value(path, labels), path)
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

func (c topicAddCounter) Inc() {
	if c.path != "ydb.topic.reader.session.errors" {
		panic("topic bulk counter unexpectedly used Inc")
	}
	c.registry.add(c.path, c.labels, 1)
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

func readerBytesInfo(
	readerInfo trace.TopicReaderInfo,
	bytes int,
) trace.TopicReaderReceiveDataResponseStartInfo {
	return trace.TopicReaderReceiveDataResponseStartInfo{
		ReaderInfo:   readerInfo,
		DataResponse: &rawtopicreader.ReadResponse{BytesSize: bytes},
	}
}

func listenerBytesInfo(
	readerInfo trace.TopicReaderInfo,
	bytes int,
	err error,
) trace.TopicListenerReceiveMessageInfo {
	return trace.TopicListenerReceiveMessageInfo{
		ReaderInfo:  readerInfo,
		MessageType: "ReadResponse",
		BytesSize:   bytes,
		Error:       err,
	}
}
