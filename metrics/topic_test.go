package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicReaderReceivedMessagesMetric(t *testing.T) {
	const metricPath = "ydb.topic.reader.received.messages"

	registry := newRecordingRegistry()
	config := recordingConfig{
		registry: registry,
		details:  trace.TopicReaderMessageEvents,
	}
	tracer := topic(config.WithSystem("ydb"))

	require.Equal(t, "counter", registry.kinds[metricPath])
	require.Equal(t, []string{"endpoint", "database", "topic", "consumer", "reader.name"},
		registry.labelNames[metricPath])

	tests := []trace.TopicReaderMessagesReceivedInfo{
		{
			Endpoint:      "node-a:2135",
			Database:      "/local",
			Topic:         "/local/topic-a",
			Consumer:      "consumer-a",
			ReaderName:    readerNamePointer("reader-a"),
			MessagesCount: 3,
		},
		{
			Endpoint:      "node-b:2135",
			Database:      "/other",
			Topic:         "/other/topic-b",
			ReaderName:    readerNamePointer("reader-b"),
			MessagesCount: 2,
		},
	}
	for _, info := range tests {
		tracer.OnReaderMessagesReceived(info)
	}

	require.Equal(t, float64(3), registry.value(metricPath, map[string]string{
		"endpoint":    "node-a:2135",
		"database":    "/local",
		"topic":       "/local/topic-a",
		"consumer":    "consumer-a",
		"reader.name": "reader-a",
	}))
	require.Equal(t, float64(2), registry.value(metricPath, map[string]string{
		"endpoint":    "node-b:2135",
		"database":    "/other",
		"topic":       "/other/topic-b",
		"consumer":    "",
		"reader.name": "reader-b",
	}))
}

func TestTopicReaderReceivedMessagesMetricDisabled(t *testing.T) {
	registry := newRecordingRegistry()
	tracer := topic(recordingConfig{
		registry: registry,
		details:  trace.TopicReaderCustomerEvents,
	})

	require.Nil(t, tracer.OnReaderMessagesReceived)

	require.Zero(t, registry.value("topic.reader.received.messages", map[string]string{
		"endpoint":    "node-a:2135",
		"database":    "/local",
		"topic":       "/local/topic-a",
		"consumer":    "consumer-a",
		"reader.name": "reader-a",
	}))
}

func TestTopicReaderReceivedMessagesMetricDescriptorAndBatchAdd(t *testing.T) {
	registry := newRecordingRegistry()
	descriptor := &recordingDescriptor{}
	config := descriptorRecordingConfig{
		recordingConfig: recordingConfig{
			registry: registry,
			details:  trace.TopicReaderMessageEvents,
		},
		descriptor: descriptor,
	}
	tracer := topic(config.WithSystem("ydb"))

	require.Equal(t, []string{
		topicReaderReceivedMessagesName,
		topicReaderDeliveredMessagesName,
		topicReaderReceivedBytesName,
		topicReaderSessionErrorsName,
		topicReaderCommitQueuedName,
		topicReaderCommitAcknowledgedName,
	}, descriptor.names)
	require.Equal(t, []string{
		topicReaderReceivedMessagesUnit,
		topicReaderDeliveredMessagesUnit,
		topicReaderReceivedBytesUnit,
		topicReaderSessionErrorsUnit,
		topicReaderCommitQueuedUnit,
		topicReaderCommitAcknowledgedUnit,
	}, descriptor.units)
	require.Equal(t, "counter", registry.kinds[topicReaderReceivedMessagesName])

	info := trace.TopicReaderMessagesReceivedInfo{
		Endpoint:      "node-a:2135",
		Database:      "/local",
		Topic:         "/local/topic-a",
		Consumer:      "consumer-a",
		ReaderName:    readerNamePointer("reader-a"),
		MessagesCount: 3,
	}
	tracer.OnReaderMessagesReceived(info)
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{MessagesCount: 0})
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{MessagesCount: -1})

	require.Equal(t, []int64{3}, descriptor.adds)
	require.Equal(t, float64(3), registry.value(topicReaderReceivedMessagesName, map[string]string{
		"endpoint":    "node-a:2135",
		"database":    "/local",
		"topic":       "/local/topic-a",
		"consumer":    "consumer-a",
		"reader.name": "reader-a",
	}))
}

type recordingDescriptor struct {
	names      []string
	units      []string
	gaugeNames []string
	gaugeUnits []string
	adds       []int64
}

type descriptorRecordingConfig struct {
	recordingConfig

	descriptor *recordingDescriptor
}

func (c descriptorRecordingConfig) WithSystem(system string) Config {
	scoped := c.recordingConfig.WithSystem(system).(recordingConfig)
	c.recordingConfig = scoped

	return c
}

func (c descriptorRecordingConfig) CounterVecWithDescriptor(
	name, unit string,
	labelNames ...string,
) CounterVec {
	c.descriptor.names = append(c.descriptor.names, name)
	c.descriptor.units = append(c.descriptor.units, unit)
	c.registry.register(name, "counter", labelNames)

	return descriptorRecordingCounterVec{
		registry:   c.registry,
		path:       name,
		descriptor: c.descriptor,
	}
}

func (c descriptorRecordingConfig) GaugeVecWithDescriptor(
	name, unit string,
	labelNames ...string,
) GaugeVec {
	c.descriptor.gaugeNames = append(c.descriptor.gaugeNames, name)
	c.descriptor.gaugeUnits = append(c.descriptor.gaugeUnits, unit)
	c.registry.register(name, "gauge", labelNames)

	return descriptorRecordingGaugeVec{registry: c.registry, path: name}
}

type descriptorRecordingCounterVec struct {
	registry   *recordingRegistry
	path       string
	descriptor *recordingDescriptor
}

func (v descriptorRecordingCounterVec) With(labels map[string]string) Counter {
	return descriptorRecordingCounter{
		registry:   v.registry,
		path:       v.path,
		labels:     labels,
		descriptor: v.descriptor,
	}
}

type descriptorRecordingCounter struct {
	registry   *recordingRegistry
	path       string
	labels     map[string]string
	descriptor *recordingDescriptor
}

func (c descriptorRecordingCounter) Inc() {
	c.registry.add(c.path, c.labels, 1)
}

func (c descriptorRecordingCounter) Add(delta int64) {
	c.descriptor.adds = append(c.descriptor.adds, delta)
	c.registry.add(c.path, c.labels, float64(delta))
}

type descriptorRecordingGaugeVec struct {
	registry *recordingRegistry
	path     string
}

func (v descriptorRecordingGaugeVec) With(labels map[string]string) Gauge {
	return recordingGauge{registry: v.registry, path: v.path, labels: labels}
}
