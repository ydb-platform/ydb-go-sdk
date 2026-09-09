package topiclistenerinternal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicListenerReconnectorDoesNotReconnectBeforeHandlerReturns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	first := newReconnectorTestStream("session-1")
	second := newReconnectorTestStream("session-2")
	client := newReconnectorTestClient(
		reconnectorTestConnectResult{stream: first},
		reconnectorTestConnectResult{stream: second},
	)
	listener, handler := newTestTopicListenerReconnector(t, client)
	readMessagesRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseReadMessages := func() {
		releaseOnce.Do(func() { close(readMessagesRelease) })
	}
	t.Cleanup(releaseReadMessages)
	handler.readMessagesRelease = readMessagesRelease

	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)

	first.messages <- testStartPartitionSessionMessage()
	first.messages <- testReadMessage()
	select {
	case <-handler.readMessages:
	case <-ctx.Done():
		t.Fatal("timeout waiting for blocked read handler")
	}

	listener.m.Lock()
	streamListener := listener.streamListener
	listener.m.Unlock()
	require.NotNil(t, streamListener)
	first.recvErr <- xerrors.Transport(status.Error(codes.Unavailable, "stream failed"))
	require.Eventually(t, streamListener.closing.Load, time.Second, time.Millisecond)

	select {
	case call := <-client.calls:
		t.Fatalf("reconnected while handler was still running: call %d", call)
	case <-time.After(1100 * time.Millisecond):
	}

	releaseReadMessages()
	waitReconnectorCall(ctx, t, client, 1)
	closeTestTopicListenerReconnector(ctx, t, listener)
}

func TestTopicListenerReconnectorWaitStopWaitsForHandlerAfterCloseTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream := newReconnectorTestStream("session-1")
	client := newReconnectorTestClient(reconnectorTestConnectResult{stream: stream})
	listener, handler := newTestTopicListenerReconnector(t, client)
	readMessagesRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseReadMessages := func() {
		releaseOnce.Do(func() { close(readMessagesRelease) })
	}
	t.Cleanup(releaseReadMessages)
	handler.readMessagesRelease = readMessagesRelease

	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)
	stream.messages <- testStartPartitionSessionMessage()
	stream.messages <- testReadMessage()
	select {
	case <-handler.readMessages:
	case <-ctx.Done():
		t.Fatal("timeout waiting for blocked read handler")
	}

	closeCtx, closeCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	closeErr := listener.Close(closeCtx, ErrUserCloseTopic)
	closeCancel()
	require.ErrorIs(t, closeErr, context.DeadlineExceeded)

	waitCtx, waitCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	require.ErrorIs(t, listener.WaitStop(waitCtx), context.DeadlineExceeded)
	waitCancel()

	releaseReadMessages()
	require.NoError(t, listener.WaitStop(ctx))
}

func TestTopicListenerReconnectorCloseTraceCanCallClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream := newReconnectorTestStream("session-1")
	client := newReconnectorTestClient(reconnectorTestConnectResult{stream: stream})
	listener, _ := newTestTopicListenerReconnector(t, client)

	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)
	listener.m.Lock()
	streamListener := listener.streamListener
	listener.m.Unlock()
	require.NotNil(t, streamListener)

	traceResult := make(chan error, 1)
	var traceOnce sync.Once
	streamListener.tracer.OnListenerClose = func(
		trace.TopicListenerCloseStartInfo,
	) func(trace.TopicListenerCloseDoneInfo) {
		traceOnce.Do(func() {
			closeCtx, closeCancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer closeCancel()
			traceResult <- listener.Close(closeCtx, ErrUserCloseTopic)
		})

		return nil
	}

	stream.recvErr <- xerrors.Transport(status.Error(codes.Unavailable, "stream failed"))
	select {
	case err := <-traceResult:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-ctx.Done():
		t.Fatal("listener close from trace callback did not return")
	}
	require.NoError(t, listener.WaitStop(ctx))
}

func TestStreamListenerPreservesFirstShutdownReasonWhenRecvReturnsCanceled(t *testing.T) {
	fatalErr := errors.New("fatal send error")
	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()

	stream := &canceledAfterStreamClose{
		ctx:          streamCtx,
		recvStarted:  make(chan struct{}),
		recvReturned: make(chan struct{}),
	}
	listener := &streamListener{
		stream:     stream,
		tracer:     &trace.Topic{},
		listenerID: "test-listener-id",
		streamClose: func(reason error) {
			if errors.Is(reason, fatalErr) {
				cancelStream()
				<-stream.recvReturned
			}
		},
	}
	listener.background = *background.NewWorker(context.Background(), "test-listener")
	listener.background.Start("receiver", listener.receiveMessagesLoop)
	select {
	case <-stream.recvStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for receiver")
	}

	go listener.goClose(context.Background(), fatalErr)
	select {
	case <-listener.background.Done():
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for listener shutdown")
	}

	require.ErrorIs(t, listener.background.CloseReason(), fatalErr)
}

func TestStreamListenerPreservesFirstSendReasonWhenRecvReturnsCanceled(t *testing.T) {
	fatalErr := errors.New("fatal send error")
	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()

	stream := &sendFailureAfterStreamClose{
		ctx:          streamCtx,
		recvStarted:  make(chan struct{}),
		recvReturned: make(chan struct{}),
		sendErr:      xerrors.Transport(fmt.Errorf("%w: transport failure", fatalErr)),
	}
	listener := &streamListener{
		stream:     stream,
		tracer:     &trace.Topic{},
		listenerID: "test-listener-id",
		streamClose: func(reason error) {
			if errors.Is(reason, fatalErr) {
				cancelStream()
				<-stream.recvReturned
			}
		},
	}
	listener.initVars(&atomic.Int64{})
	listener.background = *background.NewWorker(context.Background(), "test-listener")
	listener.background.Start("receiver", listener.receiveMessagesLoop)
	select {
	case <-stream.recvStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for receiver")
	}
	listener.background.Start("sender", listener.sendMessagesLoop)
	listener.sendMessage(&rawtopicreader.ReadRequest{BytesSize: 1})

	select {
	case <-listener.background.Done():
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for listener shutdown")
	}

	require.ErrorIs(t, listener.background.CloseReason(), fatalErr)
}

func TestNewStreamListenerCancellationDuringInitDoesNotHang(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &blockingInitClient{started: make(chan struct{})}
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "test-consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "test-topic"}}

	result := make(chan error, 1)
	go func() {
		_, err := newStreamListener(ctx, client, newReconnectorTestHandler(), &cfg, nil)
		result <- err
	}()

	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for stream initialization")
	}
	cancel()

	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("stream initialization did not stop after cancellation")
	}
}

type canceledAfterStreamClose struct {
	ctx          context.Context //nolint:containedctx
	recvStarted  chan struct{}
	recvReturned chan struct{}
	recvOnce     sync.Once
}

func (s *canceledAfterStreamClose) Recv() (rawtopicreader.ServerMessage, error) {
	s.recvOnce.Do(func() { close(s.recvStarted) })
	<-s.ctx.Done()
	close(s.recvReturned)

	return nil, xerrors.Transport(status.Error(codes.Canceled, "transport canceled"))
}

func (s *canceledAfterStreamClose) Send(rawtopicreader.ClientMessage) error {
	return nil
}

func (s *canceledAfterStreamClose) CloseSend() error {
	return nil
}

type sendFailureAfterStreamClose struct {
	ctx          context.Context //nolint:containedctx
	recvStarted  chan struct{}
	recvReturned chan struct{}
	sendErr      error
	recvOnce     sync.Once
}

func (s *sendFailureAfterStreamClose) Recv() (rawtopicreader.ServerMessage, error) {
	s.recvOnce.Do(func() { close(s.recvStarted) })
	<-s.ctx.Done()
	close(s.recvReturned)

	return nil, xerrors.Transport(status.Error(codes.Canceled, "transport canceled"))
}

func (s *sendFailureAfterStreamClose) Send(rawtopicreader.ClientMessage) error {
	return s.sendErr
}

func (s *sendFailureAfterStreamClose) CloseSend() error {
	return nil
}

type blockingInitClient struct {
	started chan struct{}
}

func (c *blockingInitClient) StreamRead(
	ctx context.Context,
	_ int64,
	_ *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	close(c.started)
	<-ctx.Done()

	return rawtopicreader.StreamReader{}, ctx.Err()
}
