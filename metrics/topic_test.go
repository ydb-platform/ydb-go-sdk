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
