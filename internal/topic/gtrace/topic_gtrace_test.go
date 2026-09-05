package gtrace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicOnReaderMessagesReceived(t *testing.T) {
	ctx := context.Background()
	require.NotPanics(t, func() {
		TopicOnReaderMessagesReceived(&trace.Topic{}, &ctx, "endpoint", "/local", "topic", "consumer", 3)
	})

	var actual trace.TopicReaderMessagesReceivedInfo
	TopicOnReaderMessagesReceived(&trace.Topic{
		OnReaderMessagesReceived: func(info trace.TopicReaderMessagesReceivedInfo) {
			actual = info
		},
	}, &ctx, "endpoint", "/local", "topic", "consumer", 3)
	require.Equal(t, trace.TopicReaderMessagesReceivedInfo{
		Context:       &ctx,
		Endpoint:      "endpoint",
		Database:      "/local",
		Topic:         "topic",
		Consumer:      "consumer",
		MessagesCount: 3,
	}, actual)
}

func TestComposeOnReaderMessagesReceived(t *testing.T) {
	t.Run("NilCallbacks", func(t *testing.T) {
		composed := Compose(&trace.Topic{}, &trace.Topic{})

		require.NotPanics(t, func() {
			composed.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{})
		})
	})

	t.Run("CallsBothCallbacks", func(t *testing.T) {
		var calls []string
		composed := Compose(
			&trace.Topic{OnReaderMessagesReceived: func(trace.TopicReaderMessagesReceivedInfo) {
				calls = append(calls, "lhs")
			}},
			&trace.Topic{OnReaderMessagesReceived: func(trace.TopicReaderMessagesReceivedInfo) {
				calls = append(calls, "rhs")
			}},
		)

		composed.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{})

		require.Equal(t, []string{"lhs", "rhs"}, calls)
	})

	t.Run("RecoversPanic", func(t *testing.T) {
		var recovered any
		composed := Compose(
			&trace.Topic{OnReaderMessagesReceived: func(trace.TopicReaderMessagesReceivedInfo) {
				panic("messages received panic")
			}},
			&trace.Topic{},
			WithTopicPanicCallback(func(value any) {
				recovered = value
			}),
		)

		require.NotPanics(t, func() {
			composed.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{})
		})
		require.Equal(t, "messages received panic", recovered)
	})
}
