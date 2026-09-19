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
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamListener_WorkerCreationAndRouting(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)

	// Initially no workers should exist
	require.Empty(t, listener.workers)

	// Channel to signal when handler has been called
	handlerCalled := make(chan struct{})

	// Set up mock expectations - the worker will call OnStartPartitionSessionRequest
	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, event *PublicEventStartPartitionSession) error {
		event.Confirm()
		close(handlerCalled)

		return nil
	})

	// Send StartPartition message - should create a worker
	err := listener.routeMessage(ctx, &rawtopicreader.StartPartitionSessionRequest{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		PartitionSession: rawtopicreader.PartitionSession{
			PartitionSessionID: 100,
			Path:               "test-topic",
			PartitionID:        1,
		},
		CommittedOffset: 10,
		PartitionOffsets: rawtopiccommon.OffsetRange{
			Start: 5,
			End:   15,
		},
	})
	require.NoError(t, err)

	// Should have created a worker
	require.Len(t, listener.workers, 1)

	// Waiting for add session to internals
	xtest.WaitChannelClosed(t, handlerCalled)

	// Verify session was added
	session, err := listener.sessions.Get(100)
	require.NoError(t, err)
	require.NotNil(t, session)
	require.Equal(t, "test-topic", session.Topic)
	require.Equal(t, int64(1), session.PartitionID)
}

func TestStreamListener_RoutingToExistingWorker(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)

	// Set up mock expectations for StartPartition
	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, event *PublicEventStartPartitionSession) error {
		event.Confirm()

		return nil
	})

	calledHandlerOnReadMessages := make(chan struct{}, 1)
	// Set up mock expectation for OnReadMessages which will be called when ReadResponse is processed
	EventHandlerMock(e).EXPECT().OnReadMessages(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
		close(calledHandlerOnReadMessages)

		// Just return nil to acknowledge receipt
		return nil
	})

	// Create a worker first
	err := listener.routeMessage(ctx, &rawtopicreader.StartPartitionSessionRequest{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		PartitionSession: rawtopicreader.PartitionSession{
			PartitionSessionID: 100,
			Path:               "test-topic",
			PartitionID:        1,
		},
		CommittedOffset: 10,
		PartitionOffsets: rawtopiccommon.OffsetRange{
			Start: 5,
			End:   15,
		},
	})
	require.NoError(t, err)
	require.Len(t, listener.workers, 1)

	// Now send a ReadResponse - should route to the existing worker without error
	// We test routing logic, not async processing (since background workers aren't started in test)
	err = listener.routeMessage(ctx, &rawtopicreader.ReadResponse{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		BytesSize: 100,
		PartitionData: []rawtopicreader.PartitionData{
			{
				PartitionSessionID: 100, // Same as the worker's partition
				Batches: []rawtopicreader.Batch{
					{
						Codec:            rawtopiccommon.CodecRaw,
						ProducerID:       "test-producer",
						WriteSessionMeta: nil,
						MessageData: []rawtopicreader.MessageData{
							{
								Offset:           10,
								SeqNo:            1,
								CreatedAt:        testTime(0),
								Data:             []byte("test"),
								UncompressedSize: 4,
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Should still have exactly one worker
	require.Len(t, listener.workers, 1)

	// Verify the worker exists - since there's only one worker, we can get it by iterating
	var worker *PartitionWorker
	listener.m.WithLock(func() {
		for _, w := range listener.workers {
			worker = w

			break
		}
	})
	require.NotNil(t, worker)

	// The worker should have received the batch message in its queue
	// We can't easily check queue state, but the routing completed without error
	// which means the batch was successfully created and sent to the worker
	xtest.WaitChannelClosed(t, calledHandlerOnReadMessages)
}

func TestStreamListener_CloseWorkers(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)

	// Channel to signal when handler has been called
	handlerCalled := make(chan struct{})

	// Set up mock expectations
	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, event *PublicEventStartPartitionSession) error {
		event.Confirm()
		close(handlerCalled)

		return nil
	})

	// Create a worker
	err := listener.routeMessage(ctx, &rawtopicreader.StartPartitionSessionRequest{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		PartitionSession: rawtopicreader.PartitionSession{
			PartitionSessionID: 100,
			Path:               "test-topic",
			PartitionID:        1,
		},
		CommittedOffset: 10,
		PartitionOffsets: rawtopiccommon.OffsetRange{
			Start: 5,
			End:   15,
		},
	})
	require.NoError(t, err)
	require.Len(t, listener.workers, 1)

	// Wait for the handler to be called by the worker
	xtest.WaitChannelClosed(t, handlerCalled)

	// Close the listener - this might fail if background worker is already closed by test cleanup
	// That's expected behavior in test environment
	_ = listener.Close(ctx, errors.New("test close"))

	// Workers should be cleared
	require.Empty(t, listener.workers)
}

func TestStreamListenerCloseWaitsForRemovedWorker(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)
	worker := listener.createWorkerForPartition(PartitionSession(e))
	closedWorkers := make(chan int, 1)
	listener.tracer.OnListenerClose = func(trace.TopicListenerCloseStartInfo) func(trace.TopicListenerCloseDoneInfo) {
		return func(info trace.TopicListenerCloseDoneInfo) {
			closedWorkers <- info.WorkersClosed
		}
	}

	removed := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	originalOnStopped := worker.onStopped
	worker.onStopped = func(id rawtopicreader.PartitionSessionID, _ error) {
		originalOnStopped(id, errPartitionQueueClosed)
		close(removed)
		<-release
	}
	worker.messageQueue.Close()
	xtest.WaitChannelClosed(t, removed)

	closeCtx, cancelClose := context.WithTimeout(sf.Context(e), 20*time.Millisecond)
	require.ErrorIs(t, listener.Close(closeCtx, ErrUserCloseTopic), context.DeadlineExceeded)
	cancelClose()

	close(release)
	released = true
	require.NoError(t, listener.Close(xtest.ContextWithCommonTimeout(sf.Context(e), t), ErrUserCloseTopic))
	require.Equal(t, 1, xtest.Receive(t, closedWorkers, "closed worker trace count"))
}

func TestStreamListenerCloseWaitsForStartingWorker(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)
	startEntered := make(chan struct{})
	allowStart := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(allowStart)
		}
	}()
	listener.tracer.OnPartitionWorkerStart = func(trace.TopicPartitionWorkerStartInfo) {
		close(startEntered)
		<-allowStart
	}

	created := make(chan *PartitionWorker, 1)
	session := PartitionSession(e)
	go func() {
		created <- listener.createWorkerForPartition(session)
	}()
	xtest.WaitChannelClosed(t, startEntered)

	closeResult := make(chan error, 1)
	closeCtx := xtest.ContextWithCommonTimeout(sf.Context(e), t)
	go func() {
		closeResult <- listener.Close(closeCtx, ErrUserCloseTopic)
	}()

	select {
	case err := <-closeResult:
		t.Fatalf("Close returned before worker startup completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(allowStart)
	released = true
	require.NotNil(t, <-created)
	require.NoError(t, <-closeResult)
}

func TestStreamListenerStoppedWorkerKeepsReplacement(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	session := PartitionSession(e)
	first := listener.createWorkerForPartition(session)
	replacement := listener.createWorkerForPartition(session)
	first.messageQueue.Close()
	xtest.WaitChannelClosed(t, first.bgWorker.StopDone())

	var current *PartitionWorker
	listener.m.WithLock(func() {
		current = listener.workers[session.StreamPartitionSessionID]
	})
	require.Same(t, replacement, current)
	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
}

func TestStreamListenerMergeFailureKeepsBatchesSeparate(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	session := PartitionSession(e)
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, event *PublicEventStartPartitionSession) error {
			close(entered)
			<-release
			event.Confirm()

			return nil
		},
	)
	processed := make(chan struct{}, 2)
	EventHandlerMock(e).EXPECT().OnReadMessages(gomock.Any(), gomock.Any()).Times(2).DoAndReturn(
		func(context.Context, *PublicReadMessages) error {
			processed <- struct{}{}

			return nil
		},
	)
	worker := listener.createWorkerForPartition(session)
	worker.AddRawServerMessage(&rawtopicreader.StartPartitionSessionRequest{})
	xtest.WaitChannelClosed(t, entered)
	first, err := topicreadercommon.NewBatch(session, nil)
	require.NoError(t, err)
	second, err := topicreadercommon.NewBatch(session, nil)
	require.NoError(t, err)
	topicreadercommon.BatchSetCommitRangeForTest(first, topicreadercommon.CommitRange{
		PartitionSession: session, CommitOffsetStart: 1, CommitOffsetEnd: 2,
	})
	topicreadercommon.BatchSetCommitRangeForTest(second, topicreadercommon.CommitRange{
		PartitionSession: session, CommitOffsetStart: 4, CommitOffsetEnd: 5,
	})
	metadata := rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess}
	worker.AddMessagesBatch(metadata, first)
	worker.AddMessagesBatch(metadata, second)
	require.False(t, listener.closing.Load(), "a failed optimization must not stop the listener")
	close(release)
	released = true
	_ = xtest.Receive(t, processed, "the first batch")
	_ = xtest.Receive(t, processed, "the second batch")
	require.NoError(t, listener.Close(ctx, ErrUserCloseTopic))
}

func TestStreamListenerClosePrefersCompletedShutdown(t *testing.T) {
	shutdownErr := errors.New("shutdown failed")
	done := make(chan struct{})
	close(done)
	listener := &streamListener{shutdownDone: done, shutdownErr: shutdownErr}
	listener.closing.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, listener.Close(ctx, ErrUserCloseTopic), shutdownErr)
}

func TestNewStreamListenerWaitsForFailedInitCleanup(t *testing.T) {
	ctx := xtest.Context(t)
	initErr := status.Error(codes.Unavailable, "init response failed")
	client := &failedInitTopicClient{stream: &failedInitGRPCStream{err: initErr}}
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "test-consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "test-topic"}}
	closeStarted := make(chan struct{})
	closeRelease := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(closeRelease)
		}
	}()
	cfg.Tracer.OnListenerClose = func(trace.TopicListenerCloseStartInfo) func(trace.TopicListenerCloseDoneInfo) {
		close(closeStarted)
		<-closeRelease

		return nil
	}
	result := make(chan error, 1)
	go func() {
		_, err := newStreamListener(ctx, client, NewMockEventHandler(gomock.NewController(t)), &cfg, &atomic.Int64{})
		result <- err
	}()

	select {
	case err := <-result:
		t.Fatalf("initialization returned before cleanup began: %v", err)
	case <-closeStarted:
	case <-ctx.Done():
		t.Fatal("initialization cleanup did not begin")
	}
	select {
	case err := <-result:
		t.Fatalf("initialization returned before cleanup completed: %v", err)
	default:
	}

	close(closeRelease)
	released = true
	require.ErrorIs(t, xtest.Receive(t, result, "the initialization error"), initErr)
	require.ErrorIs(t, context.Cause(client.stream.ctx), initErr)
}

func TestNewStreamListenerCancellationDuringInitDoesNotHang(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()
	client := &blockingInitTopicClient{started: make(chan struct{})}
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "test-consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "test-topic"}}
	result := make(chan error, 1)
	go func() {
		_, err := newStreamListener(ctx, client, NewMockEventHandler(gomock.NewController(t)), &cfg, &atomic.Int64{})
		result <- err
	}()
	xtest.WaitChannelClosed(t, client.started)
	cancel()
	require.ErrorIs(t, xtest.Receive(t, result, "the canceled initialization"), context.Canceled)
}

func TestStreamListener_SendMessagesLoopIssuesReadRequestOnFreeBytes(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	streamMock := StreamMock(e)

	const bufferSize = 42
	sendDone := make(chan struct{})
	streamMock.EXPECT().Send(gomock.Any()).DoAndReturn(func(msg rawtopicreader.ClientMessage) error {
		readReq, ok := msg.(*rawtopicreader.ReadRequest)
		require.True(t, ok)
		require.Equal(t, bufferSize, readReq.BytesSize)
		close(sendDone)

		return nil
	})

	listener.background.Start("stream listener send loop", listener.sendMessagesLoop)
	defer func() {
		_ = listener.background.Close(ctx, errors.New("test finished"))
	}()

	listener.freeBytes <- bufferSize

	select {
	case <-sendDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for read request send")
	}
}

func TestStreamListener_ReadResponseReturnsCreditWhenWorkerMissing(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	streamMock := StreamMock(e)

	session := topicreadercommon.NewPartitionSession(
		ctx,
		"test-topic",
		1,
		0,
		listener.sessionID,
		100,
		1,
		rawtopiccommon.NewOffset(0),
	)
	require.NoError(t, listener.sessions.Add(session))

	const uncompressedSize = 4
	sendDone := make(chan struct{})
	streamMock.EXPECT().Send(gomock.Any()).DoAndReturn(func(msg rawtopicreader.ClientMessage) error {
		readReq, ok := msg.(*rawtopicreader.ReadRequest)
		require.True(t, ok)
		require.Equal(t, uncompressedSize, readReq.BytesSize)
		close(sendDone)

		return nil
	})

	listener.background.Start("stream listener send loop", listener.sendMessagesLoop)
	defer func() {
		_ = listener.background.Close(ctx, errors.New("test finished"))
	}()

	err := listener.routeMessage(ctx, &rawtopicreader.ReadResponse{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		BytesSize: uncompressedSize,
		PartitionData: []rawtopicreader.PartitionData{
			{
				PartitionSessionID: 100,
				Batches: []rawtopicreader.Batch{
					{
						Codec: rawtopiccommon.CodecRaw,
						MessageData: []rawtopicreader.MessageData{
							{
								Offset:           10,
								SeqNo:            1,
								CreatedAt:        testTime(0),
								Data:             []byte("test"),
								UncompressedSize: uncompressedSize,
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	select {
	case <-sendDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for read request after unrouted batch credit return")
	}
}

func TestStreamListener_CollectPendingFreeBytesCoalesces(t *testing.T) {
	listener := &streamListener{}
	listener.initVars(&atomic.Int64{})
	listener.freeBytes = make(chan int, 3)

	listener.freeBytes <- 10
	listener.freeBytes <- 25

	require.Equal(t, 40, listener.collectPendingFreeBytes(5))
}

func TestStreamListener_ReadBufferReleaseZero(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)

	require.NotPanics(t, func() {
		listener.ReadBufferRelease(0)
	})
}

func TestStreamListener_ReadResponseReturnsCreditAfterWorkerProcessing(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	streamMock := StreamMock(e)

	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, event *PublicEventStartPartitionSession) error {
		event.Confirm()

		return nil
	})

	handlerDone := make(chan struct{})
	EventHandlerMock(e).EXPECT().OnReadMessages(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, event *PublicReadMessages) error {
		close(handlerDone)

		return nil
	})

	const uncompressedSize = 4
	sendDone := make(chan struct{})
	streamMock.EXPECT().Send(gomock.Any()).DoAndReturn(func(msg rawtopicreader.ClientMessage) error {
		readReq, ok := msg.(*rawtopicreader.ReadRequest)
		if !ok {
			return nil
		}
		require.Equal(t, uncompressedSize, readReq.BytesSize)
		close(sendDone)

		return nil
	}).AnyTimes()

	listener.background.Start("stream listener send loop", listener.sendMessagesLoop)
	defer func() {
		_ = listener.background.Close(ctx, errors.New("test finished"))
	}()

	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.StartPartitionSessionRequest{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		PartitionSession: rawtopicreader.PartitionSession{
			PartitionSessionID: 100,
			Path:               "test-topic",
			PartitionID:        1,
		},
		CommittedOffset: 10,
		PartitionOffsets: rawtopiccommon.OffsetRange{
			Start: 5,
			End:   15,
		},
	}))

	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.ReadResponse{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		BytesSize: uncompressedSize,
		PartitionData: []rawtopicreader.PartitionData{
			{
				PartitionSessionID: 100,
				Batches: []rawtopicreader.Batch{
					{
						Codec: rawtopiccommon.CodecRaw,
						MessageData: []rawtopicreader.MessageData{
							{
								Offset:           10,
								SeqNo:            1,
								CreatedAt:        testTime(0),
								Data:             []byte("test"),
								UncompressedSize: uncompressedSize,
							},
						},
					},
				},
			},
		},
	}))

	xtest.WaitChannelClosed(t, handlerDone)

	select {
	case <-sendDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for read request after worker processed batch")
	}
}

func TestStreamListener_FlushPendingMessagesSendError(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	streamMock := StreamMock(e)

	streamMock.EXPECT().Send(gomock.Any()).Return(errors.New("send failed"))

	listener.background.Start("stream listener send loop", listener.sendMessagesLoop)
	defer func() {
		_ = listener.background.Close(ctx, errors.New("test finished"))
	}()

	listener.sendMessage(&rawtopicreader.ReadRequest{BytesSize: 10})

	select {
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for listener to close after send error")
	case <-listener.background.Context().Done():
	}
}

func TestStreamListener_FlushPendingMessagesEmpty(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)

	listener.flushPendingMessages(ctx)

	require.Empty(t, listener.messagesToSend)
}

func TestStreamListener_ReadResponseConversionError(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)

	err := listener.splitAndRouteReadResponse(&rawtopicreader.ReadResponse{
		PartitionData: []rawtopicreader.PartitionData{
			{PartitionSessionID: 100},
		},
	})
	require.Error(t, err)
}

func TestStreamListener_RouteStopPartitionToExistingWorker(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)

	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, event *PublicEventStartPartitionSession) error {
		event.Confirm()

		return nil
	})

	stopHandled := make(chan struct{})
	EventHandlerMock(e).EXPECT().OnStopPartitionSessionRequest(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, event *PublicEventStopPartitionSession) error {
		event.Confirm()
		close(stopHandled)

		return nil
	})

	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.StartPartitionSessionRequest{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		PartitionSession: rawtopicreader.PartitionSession{
			PartitionSessionID: 100,
			Path:               "test-topic",
			PartitionID:        1,
		},
		CommittedOffset: 10,
		PartitionOffsets: rawtopiccommon.OffsetRange{
			Start: 5,
			End:   15,
		},
	}))

	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		},
		PartitionSessionID: 100,
		Graceful:           true,
		CommittedOffset:    rawtopiccommon.NewOffset(20),
	}))

	xtest.WaitChannelClosed(t, stopHandled)
}

func TestStreamListener_ReadBufferReleaseSkipsOnShutdown(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	listener.freeBytes = make(chan int) // unbuffered: no reader after background stops

	listener.background.Start("stream listener send loop", listener.sendMessagesLoop)
	require.NoError(t, listener.background.Close(ctx, errors.New("shutdown")))

	require.NotPanics(t, func() {
		listener.ReadBufferRelease(10)
	})
}

func TestStreamListener_ReadBufferReleaseSkipsZeroSize(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)
	// Unbuffered with no reader: if the zero-size guard did not short-circuit,
	// the send would block and the test would deadlock.
	listener.freeBytes = make(chan int)

	require.NotPanics(t, func() {
		listener.ReadBufferRelease(0)
	})
}

func TestStreamListenerBeginClosePreservesFirstReason(t *testing.T) {
	ctx := xtest.Context(t)
	var closeReasons []error
	streamCtx, streamClose := context.WithCancelCause(ctx)
	listener := &streamListener{
		streamClose: func(reason error) {
			closeReasons = append(closeReasons, reason)
			streamClose(reason)
		},
		tracer: &trace.Topic{},
	}
	_ = listener.background.Context()
	firstErr := errors.New("message handler failed")

	listener.beginClose(ctx, firstErr)
	listener.beginClose(ctx, context.Canceled)
	xtest.WaitChannelClosed(t, listener.background.StopDone())

	require.Equal(t, []error{firstErr}, closeReasons)
	require.ErrorIs(t, context.Cause(streamCtx), firstErr)
	require.ErrorIs(t, listener.background.CloseReason(), firstErr)
}

func TestStreamListenerConcurrentBeginCloseKeepsStreamCause(t *testing.T) {
	ctx := xtest.Context(t)
	streamCtx, streamClose := context.WithCancelCause(ctx)
	listener := &streamListener{streamClose: streamClose, tracer: &trace.Topic{}}
	_ = listener.background.Context()
	firstErr := errors.New("first failure")
	secondErr := errors.New("second failure")
	start := make(chan struct{})
	finished := make(chan struct{}, 2)

	for _, reason := range []error{firstErr, secondErr} {
		go func() {
			<-start
			listener.beginClose(ctx, reason)
			finished <- struct{}{}
		}()
	}
	close(start)
	<-finished
	<-finished
	xtest.WaitChannelClosed(t, listener.background.StopDone())

	cause := context.Cause(streamCtx)
	require.True(t, errors.Is(cause, firstErr) || errors.Is(cause, secondErr))
	require.ErrorIs(t, listener.background.CloseReason(), cause)
}

func testTime(num int) time.Time {
	return time.Date(2000, 1, 1, 0, 0, num, 0, time.UTC)
}

type failedInitTopicClient struct {
	stream *failedInitGRPCStream
}

func (c *failedInitTopicClient) StreamRead(
	ctx context.Context, _ int64, tracer *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	c.stream.ctx = ctx

	return rawtopicreader.StreamReader{Stream: c.stream, Tracer: tracer}, nil
}

type failedInitGRPCStream struct {
	ctx context.Context //nolint:containedctx
	err error
}

func (*failedInitGRPCStream) Send(*Ydb_Topic.StreamReadMessage_FromClient) error {
	return nil
}

func (s *failedInitGRPCStream) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	return nil, s.err
}

func (*failedInitGRPCStream) CloseSend() error {
	return nil
}

type blockingInitTopicClient struct {
	started chan struct{}
}

func (c *blockingInitTopicClient) StreamRead(
	ctx context.Context, _ int64, _ *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	close(c.started)
	<-ctx.Done()

	return rawtopicreader.StreamReader{}, ctx.Err()
}
