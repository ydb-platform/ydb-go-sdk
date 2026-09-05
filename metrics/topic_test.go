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
	require.Equal(t, []string{"endpoint", "database", "topic", "consumer"},
		registry.labelNames[metricPath])

	tests := []trace.TopicReaderMessagesReceivedInfo{
		{
			Endpoint:      "node-a:2135",
			Database:      "/local",
			Topic:         "/local/topic-a",
			Consumer:      "consumer-a",
			MessagesCount: 3,
		},
		{
			Endpoint:      "node-b:2135",
			Database:      "/other",
			Topic:         "/other/topic-b",
			MessagesCount: 2,
		},
	}
	for _, info := range tests {
		tracer.OnReaderMessagesReceived(info)
	}

	require.Equal(t, float64(3), registry.value(metricPath, map[string]string{
		"endpoint": "node-a:2135",
		"database": "/local",
		"topic":    "/local/topic-a",
		"consumer": "consumer-a",
	}))
	require.Equal(t, float64(2), registry.value(metricPath, map[string]string{
		"endpoint": "node-b:2135",
		"database": "/other",
		"topic":    "/other/topic-b",
		"consumer": "",
	}))
}

func TestTopicReaderReceivedMessagesMetricDisabled(t *testing.T) {
	registry := newRecordingRegistry()
	tracer := topic(recordingConfig{
		registry: registry,
		details:  trace.TopicReaderCustomerEvents,
	})

	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
		Endpoint:      "node-a:2135",
		Database:      "/local",
		Topic:         "/local/topic-a",
		Consumer:      "consumer-a",
		MessagesCount: 3,
	})

	require.Zero(t, registry.value("topic.reader.received.messages", map[string]string{
		"endpoint": "node-a:2135",
		"database": "/local",
		"topic":    "/local/topic-a",
		"consumer": "consumer-a",
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

	require.Equal(t, topicReaderReceivedMessagesName, descriptor.name)
	require.Equal(t, topicReaderReceivedMessagesUnit, descriptor.unit)
	require.Equal(t, "counter", registry.kinds[topicReaderReceivedMessagesName])

	info := trace.TopicReaderMessagesReceivedInfo{
		Endpoint:      "node-a:2135",
		Database:      "/local",
		Topic:         "/local/topic-a",
		Consumer:      "consumer-a",
		MessagesCount: 3,
	}
	tracer.OnReaderMessagesReceived(info)
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{MessagesCount: 0})
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{MessagesCount: -1})

	require.Equal(t, []int64{3}, descriptor.adds)
	require.Equal(t, float64(3), registry.value(topicReaderReceivedMessagesName, map[string]string{
		"endpoint": "node-a:2135",
		"database": "/local",
		"topic":    "/local/topic-a",
		"consumer": "consumer-a",
	}))
}

type recordingDescriptor struct {
	name string
	unit string
	adds []int64
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
	c.descriptor.name = name
	c.descriptor.unit = unit
	c.registry.register(name, "counter", labelNames)

	return descriptorRecordingCounterVec{
		registry:   c.registry,
		path:       name,
		descriptor: c.descriptor,
	}
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
