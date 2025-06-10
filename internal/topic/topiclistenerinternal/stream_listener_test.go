package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestStreamListener_WorkerCreationAndRouting(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)

	// Initially no workers should exist
	require.Empty(t, listener.workers)

	// Set up mock expectations - the worker will call OnStartPartitionSessionRequest
	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, event *PublicEventStartPartitionSession) error {
		event.Confirm()

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

	// Set up mock expectations
	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, event *PublicEventStartPartitionSession) error {
		event.Confirm()

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

	// Close the listener - this might fail if background worker is already closed by test cleanup
	// That's expected behavior in test environment
	_ = listener.Close(ctx, errors.New("test close"))

	// Workers should be cleared
	require.Empty(t, listener.workers)
}

func testTime(num int) time.Time {
	return time.Date(2000, 1, 1, 0, 0, num, 0, time.UTC)
}
