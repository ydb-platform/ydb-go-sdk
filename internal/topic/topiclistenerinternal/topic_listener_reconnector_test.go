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
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
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
		unchanged := listener.streamListener == first
		listener.m.Unlock()
		require.True(t, unchanged, "the stream must not be replaced before the backoff expires")
	}
	clock.Advance(delay)

	require.Eventually(t, func() bool {
		listener.m.Lock()
		defer listener.m.Unlock()

		return listener.streamListener != nil && listener.streamListener != first
	}, time.Second, time.Millisecond, "the stream must be replaced after the backoff expires")
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
		_, err := listener.reconnect(ctx, streamErr)
		finished <- err
	}()

	_ = xtest.Receive(t, timers, "the reconnect backoff timer")
	clock.Advance(cfg.RetrySettings.StartTimeout + time.Nanosecond)

	err := xtest.Receive(t, finished, "the exhausted retry timeout")
	require.ErrorIs(t, err, streamErr)
	require.ErrorContains(t, err, "reconnection timeout")
	require.ErrorIs(t, err, reason)
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

func TestTopicListenerReconnectorPreservesStreamErrorContext(t *testing.T) {
	ctx := xtest.Context(t)
	cfg := NewStreamListenerConfig()
	streamErr := status.Error(codes.PermissionDenied, "read access denied")
	reason := xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("read stream failed: %w", xerrors.TransportError(streamErr))))
	listener := &TopicListenerReconnector{streamConfig: &cfg}

	_, err := listener.reconnect(ctx, reason)

	require.ErrorIs(t, err, reason)
	require.ErrorIs(t, err, streamErr)
	require.ErrorContains(t, err, "read stream failed")
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
