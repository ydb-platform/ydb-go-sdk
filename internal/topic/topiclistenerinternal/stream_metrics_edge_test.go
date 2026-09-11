package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamListenerSessionErrorSkipsUninitializedConfig(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 1)
	listener := &streamListener{
		tracer: &trace.Topic{
			OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
				events <- info
			},
		},
	}

	listener.traceSessionStop(context.Background(), errors.New("stream stopped"))
	require.True(t, suppressListenerSessionError(context.Background(), nil))

	select {
	case event := <-events:
		t.Fatalf("unexpected session error event: %+v", event)
	default:
	}
}

func TestStreamListenerForcedStopUnregistersMetricsWithoutConfirmation(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)
	source, session := setupStopMetrics(t, listener, e)

	stopHandled := make(chan *PublicEventStopPartitionSession, 1)
	EventHandlerMock(e).EXPECT().OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, event *PublicEventStopPartitionSession) error {
			stopHandled <- event

			return nil
		},
	)

	require.NoError(t, listener.routeMessage(context.Background(), &rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: session.StreamPartitionSessionID,
	}))
	select {
	case <-stopHandled:
	case <-time.After(time.Second):
		t.Fatal("listener forced stop callback did not run")
	}

	snapshot := source.Snapshot()
	require.Zero(t, snapshot.PartitionSessionCount)
	require.Zero(t, snapshot.CommitOffsetLag)
}

func TestStreamListenerForcedStopUnregistersMetricsWithGracefulStopPending(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)
	source, session := setupStopMetrics(t, listener, e)

	stopHandled := make(chan *PublicEventStopPartitionSession, 2)
	EventHandlerMock(e).EXPECT().OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).Times(2).DoAndReturn(
		func(_ context.Context, event *PublicEventStopPartitionSession) error {
			stopHandled <- event

			return nil
		},
	)

	require.NoError(t, listener.routeMessage(context.Background(), &rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: session.StreamPartitionSessionID,
		Graceful:           true,
	}))
	first := waitStopEvent(t, stopHandled)
	snapshot := source.Snapshot()
	require.Equal(t, int64(1), snapshot.PartitionSessionCount)
	require.Equal(t, int64(10), snapshot.CommitOffsetLag)

	require.NoError(t, listener.routeMessage(context.Background(), &rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: session.StreamPartitionSessionID,
		Graceful:           false,
	}))
	snapshot = source.Snapshot()
	require.Zero(t, snapshot.PartitionSessionCount)
	require.Zero(t, snapshot.CommitOffsetLag)

	first.Confirm()
	second := waitStopEvent(t, stopHandled)
	second.Confirm()
}

func TestStreamListener_LocalBufferGuardsAndPartialRelease(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)
	listener.cfg.ReaderInfo = topicreadercommon.ReaderInfo{
		Endpoint:   "node:2135",
		Database:   "/db",
		Consumer:   "consumer",
		ReaderName: readerNamePointer("reader"),
	}
	deltas := make(chan int, 8)
	listener.tracer = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			deltas <- info.MessagesDelta
		},
	}

	session := PartitionSession(e)
	session.Topic = "/topic"
	topic := session.Topic
	otherTopic := "/other-topic"
	// Releasing before the first reservation is a no-op because there is no map yet.
	listener.releaseLocalBuffer(topic, 1)
	listener.releaseLocalBuffer(topic, 0)

	require.True(t, listener.reserveLocalBuffer(topic, 3))
	require.Equal(t, 3, listenerMetricDelta(t, deltas))
	require.True(t, listener.reserveLocalBuffer(otherTopic, 2))
	require.Equal(t, 2, listenerMetricDelta(t, deltas))

	// Unknown topics and releases larger than the balance must not change ownership.
	listener.releaseLocalBuffer("/unknown-topic", 1)
	listener.releaseLocalBuffer(topic, 4)
	listenerMetricNoDelta(t, deltas)

	listener.releaseLocalBuffer(otherTopic, 2)
	require.Equal(t, -2, listenerMetricDelta(t, deltas))
	listener.releaseLocalBuffer(topic, 1)
	require.Equal(t, -1, listenerMetricDelta(t, deltas))
	listener.finalizeLocalBuffer()
	require.Equal(t, -2, listenerMetricDelta(t, deltas))

	// Finalization is terminal and idempotent for both reservations and releases.
	require.False(t, listener.reserveLocalBuffer(topic, 1))
	listener.releaseLocalBuffer(topic, 1)
	listener.finalizeLocalBuffer()
	listenerMetricNoDelta(t, deltas)

	require.NoError(t, listener.Close(context.Background(), errors.New("test finished")))
}

func TestStreamListener_LocalBufferFinalizeWithoutOutstandingMessages(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		e := fixenv.New(t)
		listener := StreamListener(e)
		listener.cfg.ReaderInfo = topicreadercommon.ReaderInfo{ReaderName: readerNamePointer("reader")}
		deltas := make(chan int, 2)
		listener.tracer = &trace.Topic{
			OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
				deltas <- info.MessagesDelta
			},
		}

		listener.finalizeLocalBuffer()
		listenerMetricNoDelta(t, deltas)
	})

	t.Run("fully released", func(t *testing.T) {
		e := fixenv.New(t)
		listener := StreamListener(e)
		listener.cfg.ReaderInfo = topicreadercommon.ReaderInfo{ReaderName: readerNamePointer("reader")}
		deltas := make(chan int, 2)
		listener.tracer = &trace.Topic{
			OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
				deltas <- info.MessagesDelta
			},
		}

		require.True(t, listener.reserveLocalBuffer("/topic", 2))
		require.Equal(t, 2, listenerMetricDelta(t, deltas))
		listener.releaseLocalBuffer("/topic", 2)
		require.Equal(t, -2, listenerMetricDelta(t, deltas))
		listener.finalizeLocalBuffer()
		listenerMetricNoDelta(t, deltas)
	})
}

func setupStopMetrics(
	t *testing.T,
	listener *streamListener,
	e fixenv.Env,
) (*topicreadercommon.ReaderMetricsSource, *topicreadercommon.PartitionSession) {
	t.Helper()
	source := topicreadercommon.NewReaderMetricsSource()
	listener.metricsSource = source
	session := PartitionSession(e)
	session.SetupMetricsSource(source)
	source.RegisterPartitionSession(session)
	source.RegisterCommit(session, 10)
	listener.createWorkerForPartition(session)
	t.Cleanup(func() {
		_ = listener.Close(context.Background(), errors.New("test cleanup"))
	})

	return source, session
}

func waitStopEvent(t *testing.T, events <-chan *PublicEventStopPartitionSession) *PublicEventStopPartitionSession {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("listener stop callback did not run")

		return nil
	}
}
