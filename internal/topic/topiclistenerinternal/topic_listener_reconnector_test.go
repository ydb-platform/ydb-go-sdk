package topiclistenerinternal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type reconnectorTestStream struct {
	ctx       context.Context //nolint:containedctx
	sessionID string
	recvErr   chan error
	messages  chan *Ydb_Topic.StreamReadMessage_FromServer
	initSent  bool
}

func newReconnectorTestStream(sessionID string) *reconnectorTestStream {
	return &reconnectorTestStream{
		sessionID: sessionID,
		recvErr:   make(chan error, 1),
		messages:  make(chan *Ydb_Topic.StreamReadMessage_FromServer, 2),
	}
}

func (s *reconnectorTestStream) Send(*Ydb_Topic.StreamReadMessage_FromClient) error {
	return nil
}

func (s *reconnectorTestStream) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	if !s.initSent {
		s.initSent = true

		return &Ydb_Topic.StreamReadMessage_FromServer{
			Status: Ydb.StatusIds_SUCCESS,
			ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_InitResponse{
				InitResponse: &Ydb_Topic.StreamReadMessage_InitResponse{
					SessionId: s.sessionID,
				},
			},
		}, nil
	}

	select {
	case err := <-s.recvErr:
		return nil, err
	case msg := <-s.messages:
		return msg, nil
	case <-s.ctx.Done():
		return nil, context.Cause(s.ctx)
	}
}

func (s *reconnectorTestStream) CloseSend() error {
	return nil
}

type reconnectorTestConnectResult struct {
	stream *reconnectorTestStream
	err    error
	block  bool
}

type reconnectorTestClient struct {
	m       sync.Mutex
	results []reconnectorTestConnectResult
	calls   chan int
	count   int
}

func newReconnectorTestClient(results ...reconnectorTestConnectResult) *reconnectorTestClient {
	return &reconnectorTestClient{
		results: results,
		calls:   make(chan int, len(results)+1),
	}
}

func (c *reconnectorTestClient) StreamRead(
	ctx context.Context,
	_ int64,
	_ *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	c.m.Lock()
	call := c.count
	c.count++
	var result reconnectorTestConnectResult
	if call < len(c.results) {
		result = c.results[call]
	} else {
		result.block = true
	}
	c.m.Unlock()

	c.calls <- call

	if result.block {
		<-ctx.Done()

		return rawtopicreader.StreamReader{}, ctx.Err()
	}
	if result.err != nil {
		return rawtopicreader.StreamReader{}, result.err
	}

	result.stream.ctx = ctx

	return rawtopicreader.StreamReader{
		Stream: result.stream,
		Tracer: &trace.Topic{},
	}, nil
}

func (c *reconnectorTestClient) callCount() int {
	c.m.Lock()
	defer c.m.Unlock()

	return c.count
}

type reconnectorTestHandler struct {
	readMessages        chan *PublicReadMessages
	readMessagesRelease <-chan struct{}
}

func newReconnectorTestHandler() *reconnectorTestHandler {
	return &reconnectorTestHandler{
		readMessages: make(chan *PublicReadMessages, 1),
	}
}

func (h *reconnectorTestHandler) OnStartPartitionSessionRequest(
	_ context.Context,
	event *PublicEventStartPartitionSession,
) error {
	event.Confirm()

	return nil
}

func (h *reconnectorTestHandler) OnReadMessages(_ context.Context, event *PublicReadMessages) error {
	h.readMessages <- event
	if h.readMessagesRelease != nil {
		<-h.readMessagesRelease
	}

	return nil
}

func (h *reconnectorTestHandler) OnStopPartitionSessionRequest(
	_ context.Context,
	event *PublicEventStopPartitionSession,
) error {
	event.Confirm()

	return nil
}

func newTestTopicListenerReconnector(
	t *testing.T,
	client TopicClient,
) (*TopicListenerReconnector, *reconnectorTestHandler) {
	t.Helper()

	cfg := NewStreamListenerConfig()
	cfg.Consumer = "test-consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "test-topic"}}
	handler := newReconnectorTestHandler()

	listener := newTopicListenerReconnector(
		client,
		&cfg,
		handler,
		xtest.FastClock(t),
	)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_ = listener.Close(closeCtx, ErrUserCloseTopic)
	})

	return listener, handler
}

func waitReconnectorCall(ctx context.Context, t *testing.T, client *reconnectorTestClient, expected int) {
	t.Helper()

	select {
	case call := <-client.calls:
		require.Equal(t, expected, call)
	case <-ctx.Done():
		t.Fatal("timeout waiting for listener connection attempt")
	}
}

func closeTestTopicListenerReconnector(ctx context.Context, t *testing.T, listener *TopicListenerReconnector) {
	t.Helper()

	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
	require.NoError(t, listener.WaitStop(ctx))
}

func testStartPartitionSessionMessage() *Ydb_Topic.StreamReadMessage_FromServer {
	return &Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_StartPartitionSessionRequest{
			StartPartitionSessionRequest: &Ydb_Topic.StreamReadMessage_StartPartitionSessionRequest{
				PartitionSession: &Ydb_Topic.StreamReadMessage_PartitionSession{
					PartitionSessionId: 1,
					Path:               "test-topic",
					PartitionId:        0,
				},
				PartitionOffsets: &Ydb_Topic.OffsetsRange{Start: 0, End: 1},
			},
		},
	}
}

func testReadMessage() *Ydb_Topic.StreamReadMessage_FromServer {
	return &Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_ReadResponse{
			ReadResponse: &Ydb_Topic.StreamReadMessage_ReadResponse{
				BytesSize: 4,
				PartitionData: []*Ydb_Topic.StreamReadMessage_ReadResponse_PartitionData{
					{
						PartitionSessionId: 1,
						Batches: []*Ydb_Topic.StreamReadMessage_ReadResponse_Batch{
							{
								Codec:      int32(Ydb_Topic.Codec_CODEC_RAW),
								ProducerId: "test-producer",
								MessageData: []*Ydb_Topic.StreamReadMessage_ReadResponse_MessageData{
									{
										Offset:           0,
										SeqNo:            1,
										Data:             []byte("test"),
										UncompressedSize: 4,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestTopicListenerReconnectorReconnectsAfterStreamFailure(t *testing.T) {
	ctx := xtest.Context(t)
	first := newReconnectorTestStream("session-1")
	second := newReconnectorTestStream("session-2")
	client := newReconnectorTestClient(
		reconnectorTestConnectResult{stream: first},
		reconnectorTestConnectResult{stream: second},
	)
	listener, handler := newTestTopicListenerReconnector(t, client)

	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)
	require.Equal(t, "session-1", listener.ReadSessionID())

	first.recvErr <- xerrors.Transport(status.Error(codes.Unavailable, "stream failed"))
	waitReconnectorCall(ctx, t, client, 1)
	require.Eventually(t, func() bool {
		return listener.ReadSessionID() == "session-2"
	}, time.Second, time.Millisecond)
	second.messages <- testStartPartitionSessionMessage()
	second.messages <- testReadMessage()

	select {
	case event := <-handler.readMessages:
		require.Len(t, event.Batch.Messages, 1)
		require.Equal(t, int64(0), event.Batch.Messages[0].Offset)
	case <-ctx.Done():
		t.Fatal("timeout waiting for message after listener reconnect")
	}

	closeTestTopicListenerReconnector(ctx, t, listener)
}

func TestTopicListenerReconnectorRetriesInitialConnection(t *testing.T) {
	ctx := xtest.Context(t)
	stream := newReconnectorTestStream("session-1")
	client := newReconnectorTestClient(
		reconnectorTestConnectResult{
			err: xerrors.Transport(status.Error(codes.Unavailable, "connect failed")),
		},
		reconnectorTestConnectResult{stream: stream},
	)
	listener, _ := newTestTopicListenerReconnector(t, client)

	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)
	waitReconnectorCall(ctx, t, client, 1)
	require.Equal(t, "session-1", listener.ReadSessionID())

	closeTestTopicListenerReconnector(ctx, t, listener)
}

func TestTopicListenerReconnectorStopsAfterUnretryableStreamFailure(t *testing.T) {
	ctx := xtest.Context(t)
	stream := newReconnectorTestStream("session-1")
	client := newReconnectorTestClient(reconnectorTestConnectResult{stream: stream})
	listener, _ := newTestTopicListenerReconnector(t, client)

	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)
	listener.m.Lock()
	streamListener := listener.streamListener
	listener.m.Unlock()
	require.NotNil(t, streamListener)

	stream.recvErr <- status.Error(codes.InvalidArgument, "invalid stream")
	err := listener.WaitStop(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "unretriable error")
	require.Equal(t, 1, client.callCount())
	require.True(t, streamListener.closing.Load())
	listener.m.Lock()
	streamListener = listener.streamListener
	listener.m.Unlock()
	require.Nil(t, streamListener)
}

func TestTopicListenerReconnectorCloseDuringReconnect(t *testing.T) {
	ctx := xtest.Context(t)
	stream := newReconnectorTestStream("session-1")
	client := newReconnectorTestClient(
		reconnectorTestConnectResult{stream: stream},
		reconnectorTestConnectResult{block: true},
	)
	listener, _ := newTestTopicListenerReconnector(t, client)

	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)
	stream.recvErr <- xerrors.Transport(status.Error(codes.Unavailable, "stream failed"))
	waitReconnectorCall(ctx, t, client, 1)

	closeTestTopicListenerReconnector(ctx, t, listener)
}

func TestTopicListenerReconnectorCloseUnblocksWaitInit(t *testing.T) {
	ctx := xtest.Context(t)
	client := newReconnectorTestClient(reconnectorTestConnectResult{block: true})
	listener, _ := newTestTopicListenerReconnector(t, client)
	waitReconnectorCall(ctx, t, client, 0)

	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
	require.ErrorIs(t, listener.WaitInit(ctx), ErrUserCloseTopic)
	require.NoError(t, listener.WaitStop(ctx))
}

func TestNewTopicListenerReconnectorAndWaitStopContextCancellation(t *testing.T) {
	ctx := xtest.Context(t)
	stream := newReconnectorTestStream("session-1")
	client := newReconnectorTestClient(reconnectorTestConnectResult{stream: stream})
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "test-consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "test-topic"}}

	listener, err := NewTopicListenerReconnector(client, &cfg, newReconnectorTestHandler())
	require.NoError(t, err)
	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)

	waitCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, listener.WaitStop(waitCtx), context.Canceled)

	closeTestTopicListenerReconnector(ctx, t, listener)
}

func TestTopicListenerReconnectorWaitInitContextCancellation(t *testing.T) {
	ctx := xtest.Context(t)
	client := newReconnectorTestClient(reconnectorTestConnectResult{block: true})
	listener, _ := newTestTopicListenerReconnector(t, client)
	waitReconnectorCall(ctx, t, client, 0)

	waitCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, listener.WaitInit(waitCtx), context.Canceled)

	closeTestTopicListenerReconnector(ctx, t, listener)
}

func TestTopicListenerReconnectorCloseWhileWaitingRetry(t *testing.T) {
	ctx := xtest.Context(t)
	clock := clockwork.NewFakeClock()
	client := newReconnectorTestClient(reconnectorTestConnectResult{
		err: xerrors.Transport(status.Error(codes.Unavailable, "connect failed")),
	})
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "test-consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "test-topic"}}
	listener := newTopicListenerReconnector(client, &cfg, newReconnectorTestHandler(), clock)
	waitReconnectorCall(ctx, t, client, 0)
	require.NoError(t, clock.BlockUntilContext(ctx, 1))

	closeTestTopicListenerReconnector(ctx, t, listener)
	require.ErrorIs(t, listener.WaitInit(ctx), ErrUserCloseTopic)
}

func TestTopicListenerReconnectorCloseReturnsDeadlineExceeded(t *testing.T) {
	ctx := xtest.Context(t)
	listenerRelease := make(chan struct{})
	streamRelease := make(chan struct{})
	t.Cleanup(func() {
		close(listenerRelease)
		close(streamRelease)
	})

	listenerStarted := make(chan struct{})
	listener := &TopicListenerReconnector{
		connectionCompleted: make(chan struct{}),
	}
	listener.background.Start("blocked listener", func(context.Context) {
		close(listenerStarted)
		<-listenerRelease
	})
	xtest.WaitChannelClosed(t, listenerStarted)

	streamStarted := make(chan struct{})
	streamListener := &streamListener{
		tracer:     &trace.Topic{},
		listenerID: "test-listener-id",
		sessionID:  "test-session-id",
	}
	streamListener.initVars(&listener.connectionIDCounter)
	streamListener.background.Start("blocked stream", func(context.Context) {
		close(streamStarted)
		<-streamRelease
	})
	streamListener.syncCommitter = topicreadercommon.NewCommitterStopped(
		streamListener.tracer,
		ctx,
		topicreadercommon.CommitModeSync,
		func(rawtopicreader.ClientMessage) error { return nil },
	)
	listener.streamListener = streamListener
	xtest.WaitChannelClosed(t, streamStarted)

	closeCtx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	require.ErrorIs(t, listener.Close(closeCtx, ErrUserCloseTopic), context.DeadlineExceeded)
}

func TestTopicListenerReconnectorStopsWhenContextCanceledDuringStreamCleanup(t *testing.T) {
	ctx := xtest.Context(t)
	stream := newReconnectorTestStream("session-1")
	client := newReconnectorTestClient(reconnectorTestConnectResult{stream: stream})
	listener, handler := newTestTopicListenerReconnector(t, client)
	readMessagesRelease := make(chan struct{})
	handler.readMessagesRelease = readMessagesRelease
	t.Cleanup(func() {
		select {
		case <-readMessagesRelease:
		default:
			close(readMessagesRelease)
		}
	})

	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)
	listener.m.Lock()
	streamListener := listener.streamListener
	listener.m.Unlock()
	require.NotNil(t, streamListener)

	stream.messages <- testStartPartitionSessionMessage()
	stream.messages <- testReadMessage()
	select {
	case <-handler.readMessages:
	case <-ctx.Done():
		t.Fatal("timeout waiting for blocked read handler")
	}

	stream.recvErr <- xerrors.Transport(status.Error(codes.Unavailable, "stream failed"))
	require.Eventually(t, streamListener.closing.Load, time.Second, time.Millisecond)

	closeCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(
		t,
		listener.background.Close(closeCtx, ErrUserCloseTopic),
		context.DeadlineExceeded,
	)
	close(readMessagesRelease)

	require.NoError(t, listener.WaitStop(ctx))
}
