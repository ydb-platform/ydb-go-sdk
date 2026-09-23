package topiclistenerinternal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
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

func TestTopicListenerReconnectorWaitsForBackoff(t *testing.T) {
	ctx := xtest.Context(t)
	clock := clockwork.NewFakeClock()
	timers := make(chan time.Duration, 1)
	cfg := NewStreamListenerConfig()
	cfg.clock = &recordingListenerClock{Clock: clock, timers: timers}
	listener, err := NewTopicListenerReconnector(
		freshStreamTopicClient{}, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()
	require.NoError(t, listener.WaitInit(ctx))

	listener.m.Lock()
	first := listener.streamListener
	listener.m.Unlock()
	first.goClose(ctx, status.Error(codes.Canceled, "Cancelled on the server side"))

	delay := xtest.Receive(t, timers, "the reconnect backoff timer")
	if delay > 0 {
		listener.m.Lock()
		noActiveStream := listener.streamListener == nil
		listener.m.Unlock()
		require.True(t, noActiveStream, "the old stream must be retired before backoff")
		require.Empty(t, listener.ReadSessionID())
	}
	clock.Advance(delay)

	require.Eventually(t, func() bool {
		listener.m.Lock()
		defer listener.m.Unlock()

		return listener.streamListener != nil && listener.streamListener != first
	}, time.Second, time.Millisecond, "the stream must be replaced after the backoff expires")
}

func TestTopicListenerReconnectorResetsRetryTimeoutAfterSuccessfulReconnect(t *testing.T) {
	ctx := xtest.Context(t)
	clock := clockwork.NewFakeClock()
	timers := make(chan time.Duration, 1)
	cfg := NewStreamListenerConfig()
	cfg.clock = &recordingListenerClock{Clock: clock, timers: timers}
	cfg.RetrySettings.StartTimeout = 10 * time.Second
	client := &countingStreamTopicClient{}
	listener, err := NewTopicListenerReconnector(
		client, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()
	require.NoError(t, listener.WaitInit(ctx))

	listener.m.Lock()
	first := listener.streamListener
	listener.m.Unlock()
	first.goClose(ctx, status.Error(codes.Unavailable, "first stream interrupted"))
	clock.Advance(xtest.Receive(t, timers, "the first reconnect backoff timer"))

	var second *streamListener
	require.Eventually(t, func() bool {
		listener.m.Lock()
		defer listener.m.Unlock()
		second = listener.streamListener

		return second != nil && second != first
	}, time.Second, time.Millisecond, "the first stream must be replaced")

	clock.Advance(cfg.RetrySettings.StartTimeout + time.Nanosecond)
	second.goClose(ctx, status.Error(codes.Unavailable, "second stream interrupted"))
	clock.Advance(xtest.Receive(t, timers, "the second reconnect backoff timer"))

	require.Eventually(t, func() bool {
		listener.m.Lock()
		defer listener.m.Unlock()

		return listener.streamListener != nil && listener.streamListener != second
	}, time.Second, time.Millisecond, "the second stream must get a fresh retry timeout")
}

func TestTopicListenerReconnectorReadSessionIDHidesClosingStream(t *testing.T) {
	stream := &streamListener{sessionID: "expired-session"}
	stream.closing.Store(true)
	listener := &TopicListenerReconnector{streamListener: stream}

	require.Empty(t, listener.ReadSessionID())
}

func TestTopicListenerReconnectorStopsRetryingAfterTimeout(t *testing.T) {
	ctx := xtest.Context(t)
	clock := clockwork.NewFakeClock()
	timers := make(chan time.Duration, 1)
	cfg := NewStreamListenerConfig()
	cfg.clock = &recordingListenerClock{Clock: clock, timers: timers}
	cfg.RetrySettings.StartTimeout = time.Second
	streamErr := status.Error(codes.Canceled, "Cancelled on the server side")
	reason := xerrors.WithStackTrace(fmt.Errorf("open stream failed: %w", xerrors.TransportError(streamErr)))
	listener := &TopicListenerReconnector{
		streamConfig:        &cfg,
		client:              freshStreamTopicClient{openError: reason},
		connectionCompleted: make(chan struct{}),
	}
	finished := make(chan error, 1)
	go func() {
		_, err := listener.retryConnect(ctx, streamErr)
		finished <- err
	}()

	_ = xtest.Receive(t, timers, "the reconnect backoff timer")
	clock.Advance(cfg.RetrySettings.StartTimeout + time.Nanosecond)

	err := xtest.Receive(t, finished, "the exhausted retry timeout")
	require.ErrorIs(t, err, streamErr)
	require.ErrorContains(t, err, "reconnection timeout")
	require.NotErrorIs(t, err, reason, "the expired retry period must prevent another connection attempt")
}

func TestTopicListenerReconnectorClampsRetryDelayToStartTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	clock := clockwork.NewFakeClock()
	timers := make(chan time.Duration, 1)
	cfg := NewStreamListenerConfig()
	cfg.clock = &recordingListenerClock{Clock: clock, timers: timers}
	cfg.RetrySettings.StartTimeout = time.Nanosecond
	listener := &TopicListenerReconnector{streamConfig: &cfg}
	finished := make(chan error, 1)
	go func() {
		_, err := listener.retryConnect(ctx, status.Error(codes.Unavailable, "connection failed"))
		finished <- err
	}()

	delay := xtest.Receive(t, timers, "the clamped reconnect backoff timer")
	require.LessOrEqual(t, delay, cfg.RetrySettings.StartTimeout)
	cancel()
	require.ErrorIs(t, xtest.Receive(t, finished, "the canceled reconnect result"), context.Canceled)
}

func TestTopicListenerReconnectorZeroStartTimeoutSkipsRetries(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	clock := clockwork.NewFakeClock()
	timers := make(chan time.Duration, 1)
	cfg.clock = &recordingListenerClock{Clock: clock, timers: timers}
	cfg.RetrySettings.StartTimeout = 0
	client := &countingStreamTopicClient{}
	listener := &TopicListenerReconnector{streamConfig: &cfg, client: client}
	reason := status.Error(codes.Unavailable, "initial connection failed")

	finished := make(chan error, 1)
	go func() {
		_, err := listener.retryConnect(ctx, reason)
		finished <- err
	}()
	var err error
	select {
	case <-timers:
		clock.Advance(time.Second)
		err = xtest.Receive(t, finished, "zero-timeout reconnect result")
	case err = <-finished:
	}
	require.ErrorIs(t, err, reason)
	require.ErrorContains(t, err, "reconnection timeout")
	require.Zero(t, client.attempts.Load())
}

func TestTopicListenerReconnectorRetryCallbackReceivesFullError(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	reason := fmt.Errorf("open stream failed: %w", xerrors.TransportError(
		status.Error(codes.Unavailable, "transport failed"),
	))
	var seen error
	cfg.RetrySettings.CheckError = func(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
		seen = args.Error

		return topic.PublicRetryDecisionStop
	}
	listener := &TopicListenerReconnector{streamConfig: &cfg}

	_, err := listener.retryConnect(ctx, reason)
	require.ErrorIs(t, err, reason)
	require.ErrorIs(t, seen, reason)
}

func TestTopicListenerReconnectorCloseDuringBackoff(t *testing.T) {
	ctx := xtest.Context(t)
	timers := make(chan time.Duration, 1)
	cfg := NewStreamListenerConfig()
	cfg.clock = &recordingListenerClock{Clock: clockwork.NewFakeClock(), timers: timers}
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
	_ = xtest.Receive(t, timers, "the reconnect backoff timer")

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
	clock := clockwork.NewFakeClock()
	timers := make(chan time.Duration, 1)
	cfg := NewStreamListenerConfig()
	cfg.clock = &recordingListenerClock{Clock: clock, timers: timers}
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
	case <-timers:
		t.Fatal("reconnect started before the retired worker finished")
	case <-time.After(50 * time.Millisecond):
	}
	require.EqualValues(t, 1, client.attempts.Load())

	close(release)
	released = true
	delay := xtest.Receive(t, timers, "the reconnect backoff timer")
	clock.Advance(delay)
	require.Eventually(t, func() bool { return client.attempts.Load() == 2 }, time.Second, time.Millisecond)
}

func TestTopicListenerReconnectorWaitsForCloseTraceBeforeReconnect(t *testing.T) {
	ctx := xtest.Context(t)
	clock := clockwork.NewFakeClock()
	timers := make(chan time.Duration, 1)
	cfg := NewStreamListenerConfig()
	cfg.clock = &recordingListenerClock{Clock: clock, timers: timers}
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
	case <-timers:
		t.Fatal("reconnect started before the close trace callback returned")
	case <-time.After(50 * time.Millisecond):
	}

	close(traceRelease)
	released = true
	delay := xtest.Receive(t, timers, "the reconnect backoff timer")
	clock.Advance(delay)
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
	clock := clockwork.NewFakeClock()
	timers := make(chan time.Duration, 1)
	cfg := NewStreamListenerConfig()
	cfg.clock = &recordingListenerClock{Clock: clock, timers: timers}
	client := &failFirstInitStatusTopicClient{status: Ydb.StatusIds_OVERLOADED}
	listener, err := NewTopicListenerReconnector(
		client, &cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	defer func() { _ = listener.Close(ctx, ErrUserCloseTopic) }()

	delay := xtest.Receive(t, timers, "the initial status retry backoff timer")
	clock.Advance(delay)

	require.NoError(t, listener.WaitInit(xtest.ContextWithCommonTimeout(ctx, t)))
	require.EqualValues(t, 2, client.attempts.Load())
	require.NotEmpty(t, listener.ReadSessionID())
}

func TestTopicListenerReconnectorCloseDuringInitialBackoff(t *testing.T) {
	ctx := xtest.Context(t)
	timers := make(chan time.Duration, 1)
	cfg := NewStreamListenerConfig()
	cfg.clock = &recordingListenerClock{Clock: clockwork.NewFakeClock(), timers: timers}
	listener, err := NewTopicListenerReconnector(
		freshStreamTopicClient{openError: status.Error(codes.Canceled, "initial stream interrupted")},
		&cfg, NewMockEventHandler(gomock.NewController(t)),
	)
	require.NoError(t, err)
	_ = xtest.Receive(t, timers, "the initial reconnect backoff timer")

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
	require.ErrorIs(t, listener.WaitStop(xtest.ContextWithCommonTimeout(ctx, t)), openErr)
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

type recordingListenerClock struct {
	clockwork.Clock

	timers chan<- time.Duration
}

func (c *recordingListenerClock) NewTimer(delay time.Duration) clockwork.Timer {
	timer := c.Clock.NewTimer(delay)
	c.timers <- delay

	return timer
}
