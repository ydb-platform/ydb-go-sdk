package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
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
	listener := &TopicListenerReconnector{
		streamConfig:        &cfg,
		client:              freshStreamTopicClient{openError: streamErr},
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

type recordingListenerClock struct {
	clockwork.Clock

	timers chan<- time.Duration
}

func (c *recordingListenerClock) NewTimer(delay time.Duration) clockwork.Timer {
	timer := c.Clock.NewTimer(delay)
	c.timers <- delay

	return timer
}
