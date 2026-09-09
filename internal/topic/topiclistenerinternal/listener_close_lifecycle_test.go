package topiclistenerinternal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicListenerReconnectorCloseWaitsForBlockedReadCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener, client, stream, handler, release := newBlockedTopicListenerReconnector(t)
	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)
	stream.messages <- testStartPartitionSessionMessage()
	stream.messages <- testReadMessage()
	waitForBlockedReadCallback(t, handler)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- listener.Close(context.Background(), ErrUserCloseTopic)
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the read callback was released: %v", err)
	case <-time.After(1100 * time.Millisecond):
	}
	select {
	case <-handler.readDone:
		t.Fatal("read callback finished before its release")
	default:
	}

	release()
	select {
	case <-handler.readDone:
	case <-ctx.Done():
		t.Fatal("timeout waiting for read callback to finish")
	}
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for Close")
	}
	require.NoError(t, listener.WaitStop(ctx))
}

func TestStreamListenerCloseWaitsForCleanupAfterDeadline(t *testing.T) {
	listener, handler, release := newBlockedStreamListener(t)
	waitForBlockedReadCallback(t, handler)

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- listener.Close(closeCtx, ErrUserCloseTopic)
	}()
	select {
	case err := <-closeDone:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Close deadline")
	}
	cancel()
	select {
	case <-handler.readDone:
		t.Fatal("read callback finished before its release")
	default:
	}

	release()
	select {
	case <-handler.readDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for read callback to finish")
	}
	require.NoError(t, listener.Close(context.Background(), ErrUserCloseTopic))
}

func TestStreamListenerCloseReportsAlreadyClosedWorker(t *testing.T) {
	listener, worker := newStoppedWorkerStreamListener(t)
	require.NoError(t, worker.Close(context.Background(), ErrUserCloseTopic))

	closeErr := listener.Close(context.Background(), ErrUserCloseTopic)
	require.ErrorIs(t, closeErr, background.ErrAlreadyClosed)
	require.Equal(t, closeErr, listener.Close(context.Background(), ErrUserCloseTopic))
}

func TestStreamListenerCloseJoinsWorkerRemovedDuringShutdown(t *testing.T) {
	listener, handler, release := newBlockedStreamListener(t)
	waitForBlockedReadCallback(t, handler)

	var worker *PartitionWorker
	listener.m.WithLock(func() {
		for _, candidate := range listener.workers {
			worker = candidate
		}
	})
	require.NotNil(t, worker)

	callbackReturn := make(chan struct{})
	var callbackReturnOnce sync.Once
	releaseCallback := func() {
		callbackReturnOnce.Do(func() { close(callbackReturn) })
	}
	t.Cleanup(releaseCallback)

	removed := make(chan struct{})
	worker.onStopped = func(id rawtopicreader.PartitionSessionID, reason error) {
		listener.onWorkerStopped(worker, id, reason)
		close(removed)
		<-callbackReturn
	}
	listener.background.Start("wait until canceled worker is removed", func(ctx context.Context) {
		<-ctx.Done()
		release()
		<-removed
	})

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- listener.Close(context.Background(), ErrUserCloseTopic)
	}()

	select {
	case <-removed:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for worker removal")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	closeErr := listener.Close(closeCtx, ErrUserCloseTopic)
	require.ErrorIs(t, closeErr, context.DeadlineExceeded)

	releaseCallback()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for listener Close")
	}
	require.NoError(t, listener.Close(context.Background(), ErrUserCloseTopic))
	select {
	case <-worker.bgWorker.StopDone():
	default:
		t.Fatal("listener Close returned while the partition worker starter was still alive")
	}
}

func TestTopicListenerReconnectorWaitStopWaitsAfterCloseDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener, client, stream, handler, release := newBlockedTopicListenerReconnector(t)
	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)
	stream.messages <- testStartPartitionSessionMessage()
	stream.messages <- testReadMessage()
	waitForBlockedReadCallback(t, handler)

	closeCtx, closeCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	closeErr := listener.Close(closeCtx, ErrUserCloseTopic)
	closeCancel()
	require.ErrorIs(t, closeErr, context.DeadlineExceeded)

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- listener.WaitStop(context.Background())
	}()
	select {
	case err := <-waitDone:
		t.Fatalf("WaitStop returned before the read callback was released: %v", err)
	case <-time.After(1100 * time.Millisecond):
	}
	select {
	case <-handler.readDone:
		t.Fatal("read callback finished before its release")
	default:
	}

	release()
	select {
	case <-handler.readDone:
	case <-ctx.Done():
		t.Fatal("timeout waiting for read callback to finish")
	}
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for WaitStop")
	}
}

type listenerLifecycleHandler struct {
	readStarted chan struct{}
	readRelease <-chan struct{}
	readDone    chan struct{}
	startedOnce sync.Once
	doneOnce    sync.Once
}

func (h *listenerLifecycleHandler) OnStartPartitionSessionRequest(
	_ context.Context,
	event *PublicEventStartPartitionSession,
) error {
	event.Confirm()

	return nil
}

func (h *listenerLifecycleHandler) OnReadMessages(_ context.Context, _ *PublicReadMessages) error {
	h.startedOnce.Do(func() { close(h.readStarted) })
	<-h.readRelease
	h.doneOnce.Do(func() { close(h.readDone) })

	return nil
}

func (h *listenerLifecycleHandler) OnStopPartitionSessionRequest(
	_ context.Context,
	event *PublicEventStopPartitionSession,
) error {
	event.Confirm()

	return nil
}

func newBlockedTopicListenerReconnector(
	t *testing.T,
) (*TopicListenerReconnector, *reconnectorTestClient, *reconnectorTestStream, *listenerLifecycleHandler, func()) {
	t.Helper()

	stream := newReconnectorTestStream("session-1")
	client := newReconnectorTestClient(reconnectorTestConnectResult{stream: stream})
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "test-consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "test-topic"}}

	releaseChannel := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseChannel) })
	}
	handler := &listenerLifecycleHandler{
		readStarted: make(chan struct{}),
		readRelease: releaseChannel,
		readDone:    make(chan struct{}),
	}
	listener := newTopicListenerReconnector(client, &cfg, handler, xtest.FastClock(t))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = listener.Close(cleanupCtx, ErrUserCloseTopic)
		_ = listener.WaitStop(cleanupCtx)
	})
	t.Cleanup(release)

	return listener, client, stream, handler, release
}

func newBlockedStreamListener(t *testing.T) (*streamListener, *listenerLifecycleHandler, func()) {
	t.Helper()

	releaseChannel := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseChannel) })
	}
	handler := &listenerLifecycleHandler{
		readStarted: make(chan struct{}),
		readRelease: releaseChannel,
		readDone:    make(chan struct{}),
	}
	listener := &streamListener{
		background: *background.NewWorker(context.Background(), "test stream listener"),
		handler:    handler,
		tracer:     &trace.Topic{},
		listenerID: "test-listener-id",
		sessionID:  "test-session-id",
	}
	listener.initVars(&atomic.Int64{})
	listener.syncCommitter = topicreadercommon.NewCommitterStopped(
		listener.tracer,
		context.Background(),
		topicreadercommon.CommitModeSync,
		func(rawtopicreader.ClientMessage) error { return nil },
	)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = listener.Close(cleanupCtx, ErrUserCloseTopic)
	})
	t.Cleanup(release)

	session := topicreadercommon.NewPartitionSession(
		context.Background(),
		"test-topic",
		0,
		0,
		listener.sessionID,
		rawtopicreader.PartitionSessionID(1),
		1,
		rawtopiccommon.NewOffset(0),
	)
	worker := listener.createWorkerForPartition(session)

	batch, err := topicreadercommon.NewBatch(session, nil)
	require.NoError(t, err)
	worker.AddMessagesBatch(
		rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
		batch,
	)

	return listener, handler, release
}

func newStoppedWorkerStreamListener(t *testing.T) (*streamListener, *PartitionWorker) {
	t.Helper()

	listener := &streamListener{
		background: *background.NewWorker(context.Background(), "test stream listener"),
		handler:    &listenerLifecycleHandler{},
		tracer:     &trace.Topic{},
		listenerID: "test-listener-id",
		sessionID:  "test-session-id",
	}
	listener.initVars(&atomic.Int64{})
	listener.syncCommitter = topicreadercommon.NewCommitterStopped(
		listener.tracer,
		context.Background(),
		topicreadercommon.CommitModeSync,
		func(rawtopicreader.ClientMessage) error { return nil },
	)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = listener.Close(cleanupCtx, ErrUserCloseTopic)
	})

	session := topicreadercommon.NewPartitionSession(
		context.Background(),
		"test-topic",
		0,
		0,
		listener.sessionID,
		rawtopicreader.PartitionSessionID(1),
		1,
		rawtopiccommon.NewOffset(0),
	)
	worker := NewPartitionWorker(
		session.StreamPartitionSessionID,
		session,
		listener,
		listener.handler,
		func(rawtopicreader.PartitionSessionID, error) {},
		listener.tracer,
		listener.listenerID,
	)
	listener.m.WithLock(func() {
		listener.workers[session.StreamPartitionSessionID] = worker
		listener.workerStateLocked(worker)
	})
	worker.Start(listener.background.Context())

	return listener, worker
}

func waitForBlockedReadCallback(t *testing.T, handler *listenerLifecycleHandler) {
	t.Helper()

	select {
	case <-handler.readStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for blocked read callback")
	}
}
