package topiclistenerinternal

import (
	"context"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamListenerMetricsAcrossForcedSessionReplacement(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	t.Cleanup(func() { require.NoError(t, listener.Close(ctx, ErrUserCloseTopic)) })
	listener.cfg.Decoders = topicreadercommon.NewMultiDecoder()

	buffer := make(chan int, 8)
	received := make(chan int, 4)
	delivered := make(chan int, 4)
	listener.tracer = &trace.Topic{
		OnListenerLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			buffer <- info.MessagesDelta
		},
		OnListenerMessagesReceived: func(info trace.TopicReaderMessagesReceivedInfo) {
			received <- info.MessagesCount
		},
		OnListenerMessagesDelivered: func(info trace.TopicReaderMessagesDeliveredInfo) {
			delivered <- info.MessagesCount
		},
	}

	started := make(chan int, 2)
	handler := EventHandlerMock(e)
	firstStart := handler.EXPECT().OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *PublicEventStartPartitionSession) error {
			started <- 1 // Unconfirmed until forced stop revokes this session.

			return nil
		})
	stop := handler.EXPECT().OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).After(firstStart.Call).
		Return(nil)
	secondStart := handler.EXPECT().OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).After(stop).
		DoAndReturn(func(_ context.Context, event *PublicEventStartPartitionSession) error {
			event.Confirm()
			started <- 2

			return nil
		})
	handler.EXPECT().OnReadMessages(gomock.Any(), gomock.Any()).After(secondStart).Return(nil)

	start := createTestStartPartitionRequest()
	start.PartitionSession.PartitionSessionID = 100
	require.NoError(t, listener.routeMessage(ctx, start))
	require.Equal(t, 1, listenerMetricDelta(t, started))
	first, err := listener.sessions.Get(100)
	require.NoError(t, err)
	require.NoError(t, listener.routeMessage(ctx, listenerMetricResponse(first, 50)))
	require.Equal(t, 1, listenerMetricDelta(t, received))
	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: 100}))

	replacement := createTestStartPartitionRequest()
	replacement.PartitionSession.PartitionSessionID = 101
	require.NoError(t, listener.routeMessage(ctx, replacement))
	require.Equal(t, 2, listenerMetricDelta(t, started))
	require.Equal(t, 1, listenerMetricDelta(t, buffer))
	require.Equal(t, -1, listenerMetricDelta(t, buffer), "revoked batch must leave the buffer before the next start")
	require.Equal(t, 50, listenerMetricDelta(t, listener.freeBytes))
	listenerMetricNoDelta(t, delivered)

	second, err := listener.sessions.Get(101)
	require.NoError(t, err)
	require.NoError(t, listener.routeMessage(ctx, listenerMetricResponse(second, 51)))
	require.Equal(t, 1, listenerMetricDelta(t, received))
	require.Equal(t, 1, listenerMetricDelta(t, delivered))
	require.Equal(t, 51, listenerMetricDelta(t, listener.freeBytes))
	require.Equal(t, 1, listenerMetricDelta(t, buffer))
	require.Equal(t, -1, listenerMetricDelta(t, buffer))

	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
	listenerMetricNoDelta(t, buffer)
	listenerMetricNoDelta(t, received)
	listenerMetricNoDelta(t, delivered)
}
