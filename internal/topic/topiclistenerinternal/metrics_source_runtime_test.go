package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamListenerMetricsSourceTracksCallbackAndPartitionStop(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	t.Cleanup(func() {
		_ = listener.Close(context.Background(), errors.New("test cleanup"))
	})
	listener.cfg = &StreamListenerConfig{
		Decoders: topicreadercommon.NewMultiDecoder(),
		ReaderInfo: topicreadercommon.ReaderInfo{
			Endpoint:   "node:2135",
			Database:   "/db",
			Consumer:   "consumer",
			ReaderName: readerNamePointer("reader"),
		},
	}
	source := topicreadercommon.NewReaderMetricsSource()
	listener.metricsSource = source
	session := PartitionSession(e)
	session.Topic = "/topic"
	session.SetupMetricsSource(source)
	source.RegisterPartitionSession(session)
	listener.createWorkerForPartition(session)

	callbackAge := make(chan time.Duration, 1)
	EventHandlerMock(e).EXPECT().OnReadMessages(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *PublicReadMessages) error {
			callbackAge <- source.Snapshot().OldestMessageAge

			return nil
		},
	)
	StreamMock(e).EXPECT().Send(gomock.Any()).AnyTimes().Return(nil)

	receivedAt := time.Now().Add(-time.Minute)
	require.NoError(t, listener.splitAndRouteReadResponseAt(listenerMetricResponse(session, 50), receivedAt))
	select {
	case age := <-callbackAge:
		require.Zero(t, age)
	case <-time.After(time.Second):
		t.Fatal("listener callback did not receive the batch")
	}

	require.Equal(t, int64(1), source.Snapshot().PartitionSessionCount)
	stopHandled := make(chan struct{})
	EventHandlerMock(e).EXPECT().OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, event *PublicEventStopPartitionSession) error {
			event.Confirm()
			close(stopHandled)

			return nil
		},
	)
	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: session.StreamPartitionSessionID,
		Graceful:           true,
	}))
	select {
	case <-stopHandled:
	case <-time.After(time.Second):
		t.Fatal("listener stop callback did not run")
	}
	require.Eventually(t, func() bool {
		return source.Snapshot().PartitionSessionCount == 0
	}, time.Second, time.Millisecond)
}

func TestTopicListenerReconnectorMetricsSourceClosesAfterTerminalStreamStop(t *testing.T) {
	terminal := make(chan error, 1)
	grpcStream := &terminalMetricsGrpcStream{terminal: terminal}
	client := &terminalMetricsTopicClient{stream: grpcStream}
	done := make(chan struct{})
	var source trace.TopicReaderMetricsSource
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "topic"}}
	cfg.ReaderInfo = topicreadercommon.ReaderInfo{
		Endpoint:   "endpoint",
		Database:   "/database",
		Consumer:   "consumer",
		ReaderName: readerNamePointer("reader"),
	}
	cfg.Tracer = &trace.Topic{
		OnReaderMetricsSource: func(
			info trace.TopicReaderMetricsSourceStartInfo,
		) func(trace.TopicReaderMetricsSourceDoneInfo) {
			require.True(t, info.Listener)
			source = info.Source

			return func(trace.TopicReaderMetricsSourceDoneInfo) {
				close(done)
			}
		},
	}
	reconnector, err := NewTopicListenerReconnector(client, &cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = reconnector.Close(context.Background(), errors.New("test cleanup"))
	})

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, reconnector.WaitInit(waitCtx))
	require.NotNil(t, source)
	terminal <- errors.New("terminal stream failure")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("metrics source was not closed after terminal stream stop")
	}
	require.Zero(t, source.Snapshot())
}

type terminalMetricsGrpcStream struct {
	terminal chan error
	ctx      context.Context //nolint:containedctx
	initSent bool
}

func (s *terminalMetricsGrpcStream) Send(*Ydb_Topic.StreamReadMessage_FromClient) error {
	return nil
}

func (s *terminalMetricsGrpcStream) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	if !s.initSent {
		s.initSent = true

		return &Ydb_Topic.StreamReadMessage_FromServer{
			Status: Ydb.StatusIds_SUCCESS,
			ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_InitResponse{
				InitResponse: &Ydb_Topic.StreamReadMessage_InitResponse{
					SessionId: "listener-session",
				},
			},
		}, nil
	}

	select {
	case err := <-s.terminal:
		return nil, err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *terminalMetricsGrpcStream) CloseSend() error {
	return nil
}

type terminalMetricsTopicClient struct {
	stream *terminalMetricsGrpcStream
}

func (c *terminalMetricsTopicClient) StreamRead(
	ctx context.Context,
	_ int64,
	tracer *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	c.stream.ctx = ctx

	return rawtopicreader.StreamReader{Stream: c.stream, Tracer: tracer}, nil
}
