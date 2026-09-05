package topicreadercommon

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTraceListenerMessagesReceived(t *testing.T) {
	ctx := context.Background()
	var actual trace.TopicListenerMessagesReceivedInfo
	tracer := &trace.Topic{
		OnListenerMessagesReceived: func(info trace.TopicListenerMessagesReceivedInfo) {
			actual = info
		},
	}

	TraceListenerMessagesReceived(ctx, tracer, ReaderInfo{
		Endpoint: "endpoint",
		Database: "/local",
		Consumer: "consumer",
	}, "topic", 3)

	require.Equal(t, trace.TopicListenerMessagesReceivedInfo{
		Context:       &ctx,
		Endpoint:      "endpoint",
		Database:      "/local",
		Topic:         "topic",
		Consumer:      "consumer",
		MessagesCount: 3,
	}, actual)
}
