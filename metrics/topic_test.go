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
			ReaderInfo: trace.TopicReaderInfo{
				Endpoint: "node-a:2135", Database: "/local", Consumer: "consumer-a", ReaderName: "reader-a",
			},
			Topic:         "/local/topic-a",
			MessagesCount: 3,
		},
		{
			ReaderInfo: trace.TopicReaderInfo{
				Endpoint: "node-b:2135", Database: "/other", ReaderName: "reader-b",
			},
			Topic:         "/other/topic-b",
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
