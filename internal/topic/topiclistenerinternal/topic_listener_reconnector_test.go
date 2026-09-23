package topiclistenerinternal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicListenerReconnectorReplacesCanceledStream(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	listener, err := NewTopicListenerReconnector(
		freshStreamTopicClient{}, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()
	require.NoError(t, listener.WaitInit(ctx))

	listener.m.Lock()
	first := listener.streamListener
	listener.m.Unlock()
	require.NotNil(t, first)

	streamErr := status.Error(codes.Canceled, "Cancelled on the server side")
	first.goClose(ctx, streamErr)
	xtest.WaitChannelClosed(t, first.background.StopDone())
	require.ErrorIs(t, first.background.CloseReason(), streamErr)

	require.Eventually(t, func() bool {
		listener.m.Lock()
		defer listener.m.Unlock()

		current := listener.streamListener

		return current != nil && current != first && current.background.Context().Err() == nil
	}, time.Second, 10*time.Millisecond, "the canceled stream must be replaced with a running stream")
}

func TestTopicListenerReconnectorStopsOnStreamError(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	listener, err := NewTopicListenerReconnector(
		freshStreamTopicClient{}, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()
	require.NoError(t, listener.WaitInit(ctx))

	listener.m.Lock()
	streamListener := listener.streamListener
	listener.m.Unlock()
	streamErr := errors.New("message handler failed")
	streamListener.goClose(ctx, streamErr)

	require.ErrorIs(t, listener.WaitStop(xtest.ContextWithCommonTimeout(ctx, t)), streamErr)
	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
}

func TestTopicListenerReconnectorCloseDuringInitialization(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	client := &blockingInitTopicClient{started: make(chan struct{})}
	listener, err := NewTopicListenerReconnector(
		client, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	_ = xtest.Receive(t, client.started, "listener stream initialization")

	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
	require.ErrorIs(t, listener.WaitInit(ctx), context.Canceled)
	require.NoError(t, listener.WaitStop(ctx))
	require.Empty(t, listener.ReadSessionID())
}

func TestTopicListenerReconnectorCloseImmediately(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	listener, err := NewTopicListenerReconnector(
		freshStreamTopicClient{}, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)

	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
	require.NoError(t, listener.WaitStop(ctx))
	require.Empty(t, listener.ReadSessionID())
}

func TestTopicListenerReconnectorRetriesFailedCommitSend(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	client := &failCommitStreamTopicClient{sendError: status.Error(codes.Unavailable, "commit send failed")}
	listener, err := NewTopicListenerReconnector(
		client, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()
	require.NoError(t, listener.WaitInit(ctx))

	listener.m.Lock()
	first := listener.streamListener
	listener.m.Unlock()
	session := topicreadercommon.NewPartitionSession(ctx, "test-topic", 0, 0, first.sessionID, 1, 1, 0)
	request := first.syncCommitter.NewCommitRequest(topicreadercommon.CommitRange{
		PartitionSession: session, CommitOffsetStart: 1, CommitOffsetEnd: 2,
	})
	request.Confirm()
	require.Eventually(t, func() bool {
		listener.m.Lock()
		defer listener.m.Unlock()

		return client.attempts.Load() >= 2 && listener.streamListener != nil &&
			listener.streamListener != first
	}, 2*time.Second, time.Millisecond)
}

func TestTopicListenerReconnectorAppliesStreamErrorBeforeReconnect(t *testing.T) {
	ctx := xtest.Context(t)
	backoffCalls := make(chan int, 1)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{delay: time.Hour, calls: backoffCalls})
	client := &countingStreamTopicClient{}
	listener, err := NewTopicListenerReconnector(
		client, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()
	require.NoError(t, listener.WaitInit(ctx))
	require.EqualValues(t, 1, client.attempts.Load())

	listener.m.Lock()
	first := listener.streamListener
	listener.m.Unlock()
	first.goClose(ctx, status.Error(codes.Canceled, "Cancelled on the server side"))

	select {
	case attempt := <-backoffCalls:
		require.Zero(t, attempt, "the closed stream error must be the first retry attempt")
	case <-time.After(time.Second):
		t.Fatal("the closed stream error was not passed to the standard retry backoff")
	}

	listener.m.Lock()
	noActiveStream := listener.streamListener == nil
	listener.m.Unlock()
	require.True(t, noActiveStream, "the old stream must be retired before backoff")
	require.Empty(t, listener.ReadSessionID())
	require.EqualValues(t, 1, client.attempts.Load(), "reconnect must wait for the seeded error backoff")
}

func TestTopicListenerReconnectorReadSessionIDHidesClosingStream(t *testing.T) {
	stream := &streamListener{sessionID: "expired-session"}
	stream.closing.Store(true)
	listener := &TopicListenerReconnector{streamListener: stream}

	require.Empty(t, listener.ReadSessionID())
}

func TestTopicListenerReconnectorRetryCallbackReceivesFullError(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	reason := fmt.Errorf("open stream failed: %w", xerrors.TransportError(
		status.Error(codes.Unavailable, "transport failed"),
	))
	var seen error
	cfg.CheckError = func(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
		seen = args.Error

		return topic.PublicRetryDecisionStop
	}
	listener := &TopicListenerReconnector{streamConfig: &cfg}

	_, err := listener.retryConnect(ctx, reason)
	require.ErrorIs(t, err, reason)
	require.ErrorIs(t, seen, reason)
}

func TestTopicListenerReconnectorRetryCallbackCanRetryPermanentError(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{})
	reason := status.Error(codes.PermissionDenied, "initial connection failed")
	cfg.CheckError = func(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
		require.ErrorIs(t, args.Error, reason)

		return topic.PublicRetryDecisionRetry
	}
	listener := &TopicListenerReconnector{
		streamConfig: &cfg,
		client:       freshStreamTopicClient{},
	}

	stream, err := listener.retryConnect(ctx, reason)
	require.NoError(t, err)
	require.NotNil(t, stream)
	require.NoError(t, stream.Close(ctx, ErrUserCloseTopic))
}

func TestTopicListenerReconnectorResetsBackoffAfterStatusCodeChange(t *testing.T) {
	ctx := xtest.Context(t)
	backoffCalls := make(chan int, 2)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{calls: backoffCalls})
	listener := &TopicListenerReconnector{
		streamConfig: &cfg,
		client: &failFirstStreamTopicClient{
			openError: status.Error(codes.Canceled, "reconnect failed"),
		},
	}

	stream, err := listener.retryConnect(ctx, status.Error(codes.Unavailable, "stream interrupted"))
	require.NoError(t, err)
	require.NotNil(t, stream)
	require.NoError(t, stream.Close(ctx, ErrUserCloseTopic))
	require.Equal(t, []int{0, 0}, []int{
		xtest.Receive(t, backoffCalls, "the initial stream error backoff"),
		xtest.Receive(t, backoffCalls, "the changed status code backoff"),
	})
}

func TestTopicListenerReconnectorUsesStandardInstantBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()
	backoffCalls := make(chan int, 1)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{delay: time.Hour, calls: backoffCalls})
	listener := &TopicListenerReconnector{
		streamConfig: &cfg,
		client:       freshStreamTopicClient{},
	}
	type connectResult struct {
		stream *streamListener
		err    error
	}
	result := make(chan connectResult, 1)
	go func() {
		stream, err := listener.retryConnect(ctx, instantListenerRetryError{})
		result <- connectResult{stream: stream, err: err}
	}()

	select {
	case attempt := <-backoffCalls:
		cancel()
		_ = xtest.Receive(t, result, "the canceled reconnect result")
		t.Fatalf("standard instant backoff was replaced at attempt %d", attempt)
	case res := <-result:
		require.NoError(t, res.err)
		require.NotNil(t, res.stream)
		require.NoError(t, res.stream.Close(xtest.Context(t), ErrUserCloseTopic))
	case <-ctx.Done():
		t.Fatal("timed out waiting for the instant reconnect")
	}
}

func TestTopicListenerReconnectorRetryCallbackPreservesInstantBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()
	backoffCalls := make(chan int, 1)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{delay: time.Hour, calls: backoffCalls})
	cfg.CheckError = func(topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
		return topic.PublicRetryDecisionRetry
	}
	listener := &TopicListenerReconnector{
		streamConfig: &cfg,
		client:       freshStreamTopicClient{},
	}
	type connectResult struct {
		stream *streamListener
		err    error
	}
	result := make(chan connectResult, 1)
	go func() {
		stream, err := listener.retryConnect(ctx, instantListenerRetryError{})
		result <- connectResult{stream: stream, err: err}
	}()

	select {
	case attempt := <-backoffCalls:
		cancel()
		_ = xtest.Receive(t, result, "the canceled reconnect result")
		t.Fatalf("retry callback replaced instant backoff at attempt %d", attempt)
	case res := <-result:
		require.NoError(t, res.err)
		require.NotNil(t, res.stream)
		require.NoError(t, res.stream.Close(xtest.Context(t), ErrUserCloseTopic))
	case <-ctx.Done():
		t.Fatal("timed out waiting for the instant reconnect")
	}
}

func TestTopicListenerReconnectorRetriesEOF(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{})
	listener := &TopicListenerReconnector{
		streamConfig: &cfg,
		client:       freshStreamTopicClient{},
	}

	stream, err := listener.retryConnect(ctx, io.EOF)
	require.NoError(t, err)
	require.NotNil(t, stream)
	require.NoError(t, stream.Close(ctx, ErrUserCloseTopic))
}

func TestTopicListenerReconnectorCloseDuringBackoff(t *testing.T) {
	ctx := xtest.Context(t)
	backoffCalls := make(chan int, 1)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{delay: time.Hour, calls: backoffCalls})
	listener, err := NewTopicListenerReconnector(
		freshStreamTopicClient{}, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()
	require.NoError(t, listener.WaitInit(ctx))

	listener.m.Lock()
	first := listener.streamListener
	listener.m.Unlock()
	first.goClose(ctx, status.Error(codes.Canceled, "stream interrupted"))
	_ = xtest.Receive(t, backoffCalls, "the reconnect backoff")

	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
	require.NoError(t, listener.WaitStop(ctx))
	require.ErrorIs(t, listener.Close(ctx, ErrUserCloseTopic), errTopicListenerClosed)
}

func TestTopicListenerReconnectorWaitStopWaitsAfterCloseDeadline(t *testing.T) {
	ctx := xtest.Context(t)
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
		<-finished
	}()

	listener := &TopicListenerReconnector{stopped: make(chan struct{})}
	listener.background.Start("blocked connection", func(context.Context) {
		defer close(listener.stopped)
		close(started)
		<-release
		close(finished)
	})
	<-started

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 20*time.Millisecond)
	require.ErrorIs(t, listener.Close(closeCtx, ErrUserCloseTopic), context.DeadlineExceeded)
	cancelClose()

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
	require.ErrorIs(t, listener.WaitStop(waitCtx), context.DeadlineExceeded)
	cancelWait()

	close(release)
	released = true
	require.NoError(t, listener.WaitStop(xtest.ContextWithCommonTimeout(ctx, t)))
}

func TestTopicListenerReconnectorKeepsTerminalErrorWhenCloseWins(t *testing.T) {
	ctx := xtest.Context(t)
	started := make(chan struct{})
	release := make(chan struct{})
	listener := &TopicListenerReconnector{stopped: make(chan struct{})}
	listener.background.Start("blocked connection", func(context.Context) {
		defer close(listener.stopped)
		close(started)
		<-release
	})
	<-started

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- listener.Close(ctx, ErrUserCloseTopic)
	}()
	<-listener.background.Done()

	terminalErr := errors.New("terminal reconnect error")
	stopResult := make(chan struct{})
	go func() {
		listener.stopWithError(context.Background(), terminalErr)
		close(stopResult)
	}()
	close(release)
	require.NoError(t, <-closeResult)
	<-stopResult
	require.ErrorIs(t, listener.WaitStop(ctx), terminalErr)
}

func TestTopicListenerReconnectorWaitStopWaitsForReadHandler(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	readStarted := make(chan struct{})
	readRelease := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(readRelease)
		}
	}()
	handler := NewMockEventHandler(gomock.NewController(t))
	handler.EXPECT().OnReadMessages(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *PublicReadMessages) error {
			close(readStarted)
			<-readRelease

			return nil
		},
	)
	listener, err := NewTopicListenerReconnector(freshStreamTopicClient{}, &cfg, handler)
	require.NoError(t, err)
	require.NoError(t, listener.WaitInit(ctx))

	listener.m.Lock()
	stream := listener.streamListener
	listener.m.Unlock()
	session := topicreadercommon.NewPartitionSession(ctx, "test-topic", 0, 0, stream.sessionID, 1, 1, 0)
	worker := stream.createWorkerForPartition(session)
	batch, err := topicreadercommon.NewBatch(session, nil)
	require.NoError(t, err)
	worker.AddMessagesBatch(rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess}, batch)
	xtest.WaitChannelClosed(t, readStarted)

	closeCtx, cancelClose := context.WithTimeout(ctx, 20*time.Millisecond)
	require.ErrorIs(t, listener.Close(closeCtx, ErrUserCloseTopic), context.DeadlineExceeded)
	cancelClose()
	waitCtx, cancelWait := context.WithTimeout(ctx, 20*time.Millisecond)
	require.ErrorIs(t, listener.WaitStop(waitCtx), context.DeadlineExceeded)
	cancelWait()

	close(readRelease)
	released = true
	require.NoError(t, listener.WaitStop(xtest.ContextWithCommonTimeout(ctx, t)))
}

func TestTopicListenerReconnectorWaitsForRetiredWorkerBeforeReconnect(t *testing.T) {
	ctx := xtest.Context(t)
	backoffCalls := make(chan int, 1)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{calls: backoffCalls})
	client := &countingStreamTopicClient{}
	listener, err := NewTopicListenerReconnector(
		client, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)

	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
		_ = listener.Close(ctx, ErrUserCloseTopic)
	}()
	require.NoError(t, listener.WaitInit(ctx))
	require.EqualValues(t, 1, client.attempts.Load())

	listener.m.Lock()
	first := listener.streamListener
	listener.m.Unlock()
	session := topicreadercommon.NewPartitionSession(ctx, "test-topic", 0, 0, first.sessionID, 1, 1, 0)
	worker := first.createWorkerForPartition(session)
	removed := make(chan struct{})
	originalOnStopped := worker.onStopped
	worker.onStopped = func(id rawtopicreader.PartitionSessionID, _ error) {
		originalOnStopped(id, status.Error(codes.Unavailable, "stream interrupted"))
		close(removed)
		<-release
	}
	worker.messageQueue.Close()
	xtest.WaitChannelClosed(t, removed)

	xtest.WaitChannelClosed(t, first.background.StopDone())
	select {
	case <-backoffCalls:
		t.Fatal("reconnect started before the retired worker finished")
	case <-time.After(50 * time.Millisecond):
	}
	require.EqualValues(t, 1, client.attempts.Load())

	close(release)
	released = true
	_ = xtest.Receive(t, backoffCalls, "the reconnect backoff")
	require.Eventually(t, func() bool { return client.attempts.Load() == 2 }, time.Second, time.Millisecond)
}

func TestTopicListenerReconnectorWaitsForCloseTraceBeforeReconnect(t *testing.T) {
	ctx := xtest.Context(t)
	backoffCalls := make(chan int, 1)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{calls: backoffCalls})
	traceDoneStarted := make(chan struct{}, 1)
	traceRelease := make(chan struct{})
	released := false
	cfg.Tracer.OnListenerClose = func(trace.TopicListenerCloseStartInfo) func(trace.TopicListenerCloseDoneInfo) {
		return func(trace.TopicListenerCloseDoneInfo) {
			select {
			case traceDoneStarted <- struct{}{}:
			default:
			}
			<-traceRelease
		}
	}
	client := &countingStreamTopicClient{}
	listener, err := NewTopicListenerReconnector(
		client, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() {
		if !released {
			close(traceRelease)
		}
		_ = listener.Close(ctx, ErrUserCloseTopic)
	}()
	require.NoError(t, listener.WaitInit(ctx))

	listener.m.Lock()
	first := listener.streamListener
	listener.m.Unlock()
	first.goClose(ctx, status.Error(codes.Unavailable, "stream interrupted"))
	_ = xtest.Receive(t, traceDoneStarted, "the stream close trace callback")
	select {
	case <-backoffCalls:
		t.Fatal("reconnect started before the close trace callback returned")
	case <-time.After(50 * time.Millisecond):
	}

	close(traceRelease)
	released = true
	_ = xtest.Receive(t, backoffCalls, "the reconnect backoff")
	require.Eventually(t, func() bool { return client.attempts.Load() == 2 }, time.Second, time.Millisecond)
}

func TestTopicListenerReconnectorCloseTraceCanCallClose(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	listener, err := NewTopicListenerReconnector(
		freshStreamTopicClient{}, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	require.NoError(t, listener.WaitInit(ctx))

	listener.m.Lock()
	stream := listener.streamListener
	listener.m.Unlock()
	traceResult := make(chan error, 1)
	stream.tracer.OnListenerClose = func(trace.TopicListenerCloseStartInfo) func(trace.TopicListenerCloseDoneInfo) {
		closeCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		traceResult <- listener.Close(closeCtx, ErrUserCloseTopic)
		cancel()

		return nil
	}
	stream.goClose(ctx, status.Error(codes.Unavailable, "stream interrupted"))
	require.ErrorIs(t, xtest.Receive(t, traceResult, "Close from the trace hook"), context.DeadlineExceeded)
	require.NoError(t, listener.WaitStop(xtest.ContextWithCommonTimeout(ctx, t)))
}

func TestTopicListenerReconnectorPreservesStreamErrorContext(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	streamErr := status.Error(codes.PermissionDenied, "read access denied")
	reason := xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("read stream failed: %w", xerrors.TransportError(streamErr))))
	listener := &TopicListenerReconnector{streamConfig: &cfg}

	_, err := listener.retryConnect(ctx, reason)

	require.ErrorIs(t, err, reason)
	require.ErrorIs(t, err, streamErr)
	require.ErrorContains(t, err, "read stream failed")
}

func TestTopicListenerReconnectorPreservesCompositeCloseError(t *testing.T) {
	streamErr := errors.New("worker failed during close")
	done := make(chan struct{})
	close(done)
	stream := &streamListener{
		shutdownDone: done,
		shutdownErr:  errors.Join(context.Canceled, streamErr),
	}
	stream.closing.Store(true)
	listener := &TopicListenerReconnector{streamListener: stream}

	listener.closeStream(stream, nil)

	require.ErrorIs(t, listener.streamCloseErr, streamErr)
}

func TestTopicListenerReconnectorRetriesInitialConnection(t *testing.T) {
	for _, code := range []codes.Code{codes.Canceled, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			ctx := xtest.Context(t)
			cfg := NewStreamListenerConfig()
			setListenerRetryBackoff(&cfg, listenerTestBackoff{})
			openErr := status.Error(code, "initial connection failed")
			client := &failFirstStreamTopicClient{openError: openErr}
			listener, err := NewTopicListenerReconnector(
				client, &cfg, NewMockEventHandler(gomock.NewController(t)),
			)
			require.NoError(t, err)
			defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()

			initCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			require.NoError(t, listener.WaitInit(initCtx))
			require.EqualValues(t, 2, client.attempts.Load())
			require.NotEmpty(t, listener.ReadSessionID())
		})
	}
}

func TestTopicListenerReconnectorRetriesTransientInitialStatus(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{})
	client := &failFirstInitStatusTopicClient{status: Ydb.StatusIds_OVERLOADED}
	listener, err := NewTopicListenerReconnector(
		client, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()

	require.NoError(t, listener.WaitInit(xtest.ContextWithCommonTimeout(ctx, t)))
	require.EqualValues(t, 2, client.attempts.Load())
	require.NotEmpty(t, listener.ReadSessionID())
}

func TestTopicListenerReconnectorCloseDuringInitialBackoff(t *testing.T) {
	ctx := xtest.Context(t)
	backoffCalls := make(chan int, 1)
	cfg := NewStreamListenerConfig()
	setListenerRetryBackoff(&cfg, listenerTestBackoff{delay: time.Hour, calls: backoffCalls})
	listener, err := NewTopicListenerReconnector(
		freshStreamTopicClient{openError: status.Error(codes.Canceled, "initial stream interrupted")},
		&cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	_ = xtest.Receive(t, backoffCalls, "the initial reconnect backoff")

	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
	require.ErrorIs(t, listener.WaitInit(ctx), context.Canceled)
	require.NoError(t, listener.WaitStop(ctx))
}

func TestTopicListenerReconnectorStopsOnInitialPermanentError(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	openErr := status.Error(codes.PermissionDenied, "initial connection failed")
	listener, err := NewTopicListenerReconnector(
		freshStreamTopicClient{openError: openErr}, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()

	require.ErrorIs(t, listener.WaitInit(xtest.ContextWithCommonTimeout(ctx, t)), openErr)
	stopErr := listener.WaitStop(xtest.ContextWithCommonTimeout(ctx, t))
	require.ErrorIs(t, stopErr, openErr)
	require.Equal(t, codes.PermissionDenied, status.Code(stopErr))
	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
}

type freshStreamTopicClient struct {
	openError error
}

type countingStreamTopicClient struct {
	attempts atomic.Int32
}

type failCommitStreamTopicClient struct {
	sendError error
	attempts  atomic.Int32
}

func (c *failCommitStreamTopicClient) StreamRead(
	ctx context.Context, id int64, tracer *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	if c.attempts.Add(1) != 1 {
		return freshStreamTopicClient{}.StreamRead(ctx, id, tracer)
	}

	return rawtopicreader.StreamReader{
		Stream: &failCommitGRPCStream{
			testInitGrpcStream: &testInitGrpcStream{sessionID: "test-session", recvContext: ctx},
			sendError:          c.sendError,
		},
		Tracer: tracer,
	}, nil
}

type failCommitGRPCStream struct {
	*testInitGrpcStream

	sendError error
}

func (s *failCommitGRPCStream) Send(message *Ydb_Topic.StreamReadMessage_FromClient) error {
	if message.GetCommitOffsetRequest() != nil {
		return s.sendError
	}

	return s.testInitGrpcStream.Send(message)
}

func (c *countingStreamTopicClient) StreamRead(
	ctx context.Context, id int64, tracer *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	c.attempts.Add(1)

	return freshStreamTopicClient{}.StreamRead(ctx, id, tracer)
}

func (c freshStreamTopicClient) StreamRead(
	ctx context.Context, _ int64, tracer *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	if c.openError != nil {
		return rawtopicreader.StreamReader{}, c.openError
	}

	return rawtopicreader.StreamReader{
		Stream: &testInitGrpcStream{sessionID: "test-session", recvContext: ctx},
		Tracer: tracer,
	}, nil
}

type failFirstStreamTopicClient struct {
	openError error
	attempts  atomic.Int32
}

type failFirstInitStatusTopicClient struct {
	status   Ydb.StatusIds_StatusCode
	attempts atomic.Int32
}

func (c *failFirstInitStatusTopicClient) StreamRead(
	ctx context.Context, id int64, tracer *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	if c.attempts.Add(1) == 1 {
		return rawtopicreader.StreamReader{
			Stream: &initStatusGRPCStream{status: c.status},
			Tracer: tracer,
		}, nil
	}

	return freshStreamTopicClient{}.StreamRead(ctx, id, tracer)
}

type initStatusGRPCStream struct {
	status Ydb.StatusIds_StatusCode
}

func (*initStatusGRPCStream) Send(*Ydb_Topic.StreamReadMessage_FromClient) error {
	return nil
}

func (s *initStatusGRPCStream) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	return &Ydb_Topic.StreamReadMessage_FromServer{Status: s.status}, nil
}

func (*initStatusGRPCStream) CloseSend() error {
	return nil
}

func (c *failFirstStreamTopicClient) StreamRead(
	ctx context.Context, id int64, tracer *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	if c.attempts.Add(1) == 1 {
		return rawtopicreader.StreamReader{}, c.openError
	}

	return freshStreamTopicClient{}.StreamRead(ctx, id, tracer)
}

type listenerTestBackoff struct {
	delay time.Duration
	calls chan<- int
}

type instantListenerRetryError struct{}

func (instantListenerRetryError) Error() string {
	return "instant listener retry"
}

func (instantListenerRetryError) Code() int32 {
	return -1
}

func (instantListenerRetryError) Name() string {
	return "instant listener retry"
}

func (instantListenerRetryError) Type() xerrors.Type {
	return xerrors.TypeRetryable
}

func (instantListenerRetryError) BackoffType() backoff.Type {
	return backoff.TypeInstant
}

func (b listenerTestBackoff) Delay(attempt int) time.Duration {
	if b.calls != nil {
		b.calls <- attempt
	}

	return b.delay
}

func setListenerRetryBackoff(cfg *StreamListenerConfig, backoff listenerTestBackoff) {
	cfg.retryOptions = []retry.Option{
		retry.WithFastBackoff(backoff),
		retry.WithSlowBackoff(backoff),
	}
}
