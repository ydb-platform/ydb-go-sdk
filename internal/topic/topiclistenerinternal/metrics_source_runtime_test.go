package topiclistenerinternal

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
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
			ReaderName: "reader",
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
	require.NoError(t, listener.splitAndRouteReadResponse(listenerMetricResponse(session, 50), receivedAt))
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
	}, time.Time{}))
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
		ReaderName: "reader",
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

func TestTopicListenerReconnectorMetricsSourceDoneCanCloseAfterTerminalStop(t *testing.T) {
	terminal := make(chan error, 1)
	grpcStream := &terminalMetricsGrpcStream{terminal: terminal}
	client := &terminalMetricsTopicClient{stream: grpcStream}
	done := make(chan struct{})
	closeResult := make(chan error, 1)
	var reconnector *TopicListenerReconnector
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "topic"}}
	cfg.ReaderInfo = topicreadercommon.ReaderInfo{
		Endpoint:   "endpoint",
		Database:   "/database",
		Consumer:   "consumer",
		ReaderName: "reader",
	}
	cfg.Tracer = &trace.Topic{
		OnReaderMetricsSource: func(trace.TopicReaderMetricsSourceStartInfo) func(trace.TopicReaderMetricsSourceDoneInfo) {
			return func(trace.TopicReaderMetricsSourceDoneInfo) {
				close(done)
				closeResult <- reconnector.Close(context.Background(), errors.New("reentrant close"))
			}
		},
	}

	var err error
	reconnector, err = NewTopicListenerReconnector(client, &cfg, nil)
	require.NoError(t, err)

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, reconnector.WaitInit(waitCtx))
	terminal <- errors.New("terminal stream failure")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("listener metrics source done callback was not called")
	}
	select {
	case <-closeResult:
	case <-time.After(time.Second):
		t.Fatal("listener close did not complete from reentrant metrics callback")
	}
}

func TestTopicListenerReconnectorMetricsSourceDoneCanCloseAfterInitFailure(t *testing.T) {
	done := make(chan struct{})
	closeResult := make(chan error, 1)
	ready := make(chan struct{})
	var reconnector *TopicListenerReconnector
	connectErr := errors.New("initial stream failure")
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "topic"}}
	cfg.Tracer = &trace.Topic{
		OnReaderMetricsSource: func(trace.TopicReaderMetricsSourceStartInfo) func(trace.TopicReaderMetricsSourceDoneInfo) {
			return func(trace.TopicReaderMetricsSourceDoneInfo) {
				<-ready
				close(done)
				closeResult <- reconnector.Close(context.Background(), errors.New("reentrant close"))
			}
		},
	}

	var err error
	reconnector, err = NewTopicListenerReconnector(&failingTopicClient{err: connectErr}, &cfg, nil)
	require.NoError(t, err)
	close(ready)
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, reconnector.WaitInit(waitCtx), connectErr)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("listener metrics source done callback was not called after init failure")
	}
	select {
	case <-closeResult:
	case <-time.After(time.Second):
		t.Fatal("listener close did not complete from init-failure metrics callback")
	}
}

func TestTopicListenerReconnectorExplicitCloseOwnsMetricsSourceFinalization(t *testing.T) {
	callbackStarted := make(chan struct{})
	callbackRelease := make(chan struct{})
	workerRelease := make(chan struct{})
	closeResult := make(chan error, 1)
	var callbackCalls atomic.Int32

	lr := &TopicListenerReconnector{
		background:    *background.NewWorker(context.Background(), "metrics-source-explicit-close"),
		metricsSource: topicreadercommon.NewReaderMetricsSource(),
		metricsSourceDone: func() {
			callbackCalls.Add(1)
			close(callbackStarted)
			<-callbackRelease
		},
	}
	lr.background.Start("blocked-worker", func(context.Context) {
		<-workerRelease
	})

	// Use a real watcher argument, but invoke the watcher after the parent
	// cancellation is observable so the explicit close ownership check is
	// deterministic.
	sl := &streamListener{background: *background.NewWorker(context.Background(), "metrics-source-watcher")}
	go func() {
		closeResult <- lr.Close(context.Background(), errors.New("explicit close"))
	}()
	select {
	case <-lr.background.Done():
	case <-time.After(time.Second):
		t.Fatal("listener close did not cancel its background worker")
	}

	watcherDone := make(chan struct{})
	go func() {
		lr.waitMetricsSourceStop(sl)
		close(watcherDone)
	}()
	select {
	case <-watcherDone:
		require.Zero(t, callbackCalls.Load())
	case <-time.After(time.Second):
		t.Fatal("metrics source watcher did not return during explicit close")
	}

	close(workerRelease)
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("explicit close did not invoke metrics source done callback")
	}
	select {
	case err := <-closeResult:
		t.Fatalf("listener close returned before metrics source done callback release: %v", err)
	default:
	}

	close(callbackRelease)
	select {
	case err := <-closeResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("listener close did not complete after metrics source done callback release")
	}
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
