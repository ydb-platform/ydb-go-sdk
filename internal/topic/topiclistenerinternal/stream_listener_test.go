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
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
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

func TestStreamListener_ReleaseReadBufferZero(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)

	require.NotPanics(t, func() {
		listener.releaseReadBuffer(0)
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

func TestStreamListener_ReleaseReadBufferSkipsOnShutdown(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	listener.freeBytes = make(chan int) // unbuffered: no reader after background stops

	listener.background.Start("stream listener send loop", listener.sendMessagesLoop)
	require.NoError(t, listener.background.Close(ctx, errors.New("shutdown")))

	require.NotPanics(t, func() {
		listener.releaseReadBuffer(10)
	})
}

func TestStreamListener_ReleaseReadBufferSkipsZeroSize(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)
	// Unbuffered with no reader: if the zero-size guard did not short-circuit,
	// the send would block and the test would deadlock.
	listener.freeBytes = make(chan int)

	require.NotPanics(t, func() {
		listener.releaseReadBuffer(0)
	})
}

func testTime(num int) time.Time {
	return time.Date(2000, 1, 1, 0, 0, num, 0, time.UTC)
}
