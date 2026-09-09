package topicreadercommon

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTraceMessagesDelivered(t *testing.T) {
	ctx := context.Background()
	var actual trace.TopicReaderMessagesDeliveredInfo

	TraceMessagesDelivered(
		ctx,
		&trace.Topic{OnReaderMessagesDelivered: func(info trace.TopicReaderMessagesDeliveredInfo) {
			actual = info
		}},
		ReaderInfo{
			Endpoint:   "configured:2135",
			Database:   "/local",
			Consumer:   "consumer",
			ReaderName: readerNamePointer("reader"),
			Listener:   true,
		},
		"/local/topic",
		3,
	)

	require.NotNil(t, actual.Context)
	require.Equal(t, ctx, *actual.Context)
	require.Equal(t, "configured:2135", actual.Endpoint)
	require.Equal(t, "/local", actual.Database)
	require.Equal(t, "/local/topic", actual.Topic)
	require.Equal(t, "consumer", actual.Consumer)
	require.Equal(t, readerNamePointer("reader"), actual.ReaderName)
	require.True(t, actual.Listener)
	require.Equal(t, 3, actual.MessagesCount)
}
