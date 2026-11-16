package topiclistenerinternal

import (
	"context"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamListenerForcedStopCancelsOnlyRevokedSessionImmediately(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	firstStartCalled := make(chan struct{})
	secondStartCalled := make(chan struct{})
	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, event *PublicEventStartPartitionSession) error {
			if event.PartitionSession.PartitionSessionID == 1 {
				close(firstStartCalled)
			} else {
				close(secondStartCalled)
			}

			// Keep the first start unconfirmed so its worker cannot process the queued stop.
			return nil
		}).AnyTimes()
	EventHandlerMock(e).EXPECT().OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, event *PublicEventStopPartitionSession) error {
			event.Confirm()

			return nil
		}).AnyTimes()

	firstStart := createTestStartPartitionRequest()
	firstStart.PartitionSession.PartitionSessionID = 100
	require.NoError(t, listener.routeMessage(ctx, firstStart))
	xtest.WaitChannelClosed(t, firstStartCalled)

	secondStart := createTestStartPartitionRequest()
	secondStart.PartitionSession.PartitionSessionID = 101
	require.NoError(t, listener.routeMessage(ctx, secondStart))
	firstSession, err := listener.sessions.Get(firstStart.PartitionSession.PartitionSessionID)
	require.NoError(t, err)
	secondSession, err := listener.sessions.Get(secondStart.PartitionSession.PartitionSessionID)
	require.NoError(t, err)

	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: firstStart.PartitionSession.PartitionSessionID,
		Graceful:           false,
	}))

	require.NoError(t, secondSession.Context().Err(), "another session for the same partition must remain active")
	require.ErrorIs(t, firstSession.Context().Err(), context.Canceled,
		"forced stop must revoke the session before waiting for its queued callbacks")
	xtest.WaitChannelClosed(t, secondStartCalled)
}

func TestStreamListenerReusesWorkerOnlyForTheSameTopicAndPartition(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	firstSession := topicreadercommon.NewPartitionSession(ctx, "topic-a", 7, 0, "read-session", 100, 1, 0)
	secondSession := topicreadercommon.NewPartitionSession(ctx, "topic-a", 7, 0, "read-session", 101, 2, 0)
	otherPartitionSession := topicreadercommon.NewPartitionSession(ctx, "topic-a", 8, 0, "read-session", 102, 3, 0)
	otherTopicSession := topicreadercommon.NewPartitionSession(ctx, "topic-b", 7, 0, "read-session", 103, 4, 0)

	firstWorker := listener.createWorkerForPartition(firstSession)
	secondWorker := listener.createWorkerForPartition(secondSession)
	otherPartitionWorker := listener.createWorkerForPartition(otherPartitionSession)
	otherTopicWorker := listener.createWorkerForPartition(otherTopicSession)

	require.NotNil(t, firstWorker)
	require.Same(t, firstWorker, secondWorker,
		"successive sessions for the same topic and partition must share one FIFO worker")
	require.NotSame(t, firstWorker, otherPartitionWorker, "different partitions must process callbacks independently")
	require.NotSame(t, firstWorker, otherTopicWorker, "the same partition ID in different topics must remain independent")
	require.NotSame(t, otherPartitionWorker, otherTopicWorker)
	require.Len(t, listener.workers, 3, "one worker must serve each distinct topic and partition")
}

func TestStreamListenerForcedStopInterruptsPendingGracefulConfirmation(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	starts := make(chan *PublicEventStartPartitionSession, 2)
	type stopCall struct {
		err   error
		event *PublicEventStopPartitionSession
	}
	stops := make(chan stopCall, 2)
	EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, event *PublicEventStartPartitionSession) error {
			event.Confirm()
			starts <- event

			return nil
		}).Times(2)
	EventHandlerMock(e).EXPECT().OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, event *PublicEventStopPartitionSession) error {
			// Neither the pending graceful stop nor the forced stop is confirmed.
			stops <- stopCall{err: ctx.Err(), event: event}

			return nil
		}).Times(2)

	firstStart := createTestStartPartitionRequest()
	firstStart.PartitionSession.PartitionSessionID = 100
	require.NoError(t, listener.routeMessage(ctx, firstStart))
	firstStarted := xtest.Receive(t, starts, "the first partition session")
	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: firstStart.PartitionSession.PartitionSessionID,
		Graceful:           true,
	}))
	gracefulStop := xtest.Receive(t, stops, "the unconfirmed graceful stop")
	require.True(t, gracefulStop.event.Graceful)
	require.Equal(t, firstStarted.PartitionSession, gracefulStop.event.PartitionSession)
	require.NoError(t, gracefulStop.err, "graceful stop must allow the session to finish committing")

	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: firstStart.PartitionSession.PartitionSessionID,
		Graceful:           false,
	}))
	forcedStop := xtest.Receive(t, stops, "the forced stop after cancellation of the graceful wait")
	require.False(t, forcedStop.event.Graceful)
	require.Equal(t, firstStarted.PartitionSession, forcedStop.event.PartitionSession)
	require.ErrorIs(t, forcedStop.err, context.Canceled)

	secondStart := createTestStartPartitionRequest()
	secondStart.PartitionSession.PartitionSessionID = 101
	require.NoError(t, listener.routeMessage(ctx, secondStart))
	secondStarted := xtest.Receive(t, starts, "the replacement session without confirming the forced stop")
	secondSession, err := listener.sessions.Get(secondStart.PartitionSession.PartitionSessionID)
	require.NoError(t, err)
	require.Equal(t, secondSession.ToPublic(), secondStarted.PartitionSession)
	require.NotEqual(t,
		firstStarted.PartitionSession.PartitionSessionID, secondStarted.PartitionSession.PartitionSessionID,
	)
	require.NoError(t, secondSession.Context().Err())
}

func TestPartitionWorkerExpiredCommitAfterRevocationDoesNotStopPartition(t *testing.T) {
	e := fixenv.New(t)
	listener := StreamListener(e)
	batch := createTestBatchWithBufferBytes(t, 1)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	var commitErr error
	EventHandlerMock(e).EXPECT().OnReadMessages(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, event *PublicReadMessages) error {
			// Simulate receiver revocation while the callback is processing its batch.
			session.Close()
			commitErr = event.ConfirmWithAck(context.Background())

			return commitErr
		})
	worker := NewPartitionWorker(
		session.StreamPartitionSessionID, session, listener, EventHandlerMock(e), nil,
		listener.tracer, listener.listenerID,
	)

	err := worker.processUnifiedMessage(sf.Context(e), unifiedMessage{BatchMessage: &batchMessage{
		ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
		Batch:                 batch,
	}})

	require.ErrorIs(t, commitErr, topicreadercommon.ErrPublicCommitSessionToExpiredSession)
	require.NoError(t, err, "expected revocation must leave the partition worker available for the next session")
}

func TestPartitionWorkerRevokedQueuedBatchReturnsBufferCreditOnce(t *testing.T) {
	ctx := xtest.Context(t)
	const batchBytes = 123
	batch := createTestBatchWithBufferBytes(t, batchBytes)
	session := topicreadercommon.BatchGetPartitionSession(batch)
	sender := newMockMessageSender()
	worker := NewPartitionWorker(
		session.StreamPartitionSessionID, session, sender, NewMockEventHandler(gomock.NewController(t)),
		func(rawtopicreader.PartitionSessionID, error) {}, &trace.Topic{}, "test-listener",
	)
	worker.AddMessagesBatch(rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess}, batch)
	session.Close()
	require.NoError(t, worker.Close(ctx, ErrUserCloseTopic))

	worker.receiveMessagesLoop(ctx)

	require.Equal(t, []int{batchBytes}, sender.GetFreedBuffer(),
		"skipping a revoked batch and draining the closed queue must return its exact credit only once")
}

func TestStreamListenerCommitAcknowledgementsKeepSessionIdentity(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)
	firstSession := topicreadercommon.NewPartitionSession(ctx, "topic-a", 7, 0, "read-session", 100, 1, 10)
	secondSession := topicreadercommon.NewPartitionSession(ctx, "topic-a", 7, 0, "read-session", 101, 2, 10)
	require.NoError(t, listener.sessions.Add(firstSession))
	require.NoError(t, listener.sessions.Add(secondSession))
	require.Same(t, listener.createWorkerForPartition(firstSession), listener.createWorkerForPartition(secondSession))
	firstSession.Close()

	sent := make(chan rawtopicreader.ClientMessage, 1)
	StreamMock(e).EXPECT().Send(gomock.Any()).DoAndReturn(func(msg rawtopicreader.ClientMessage) error {
		sent <- msg

		return nil
	})
	request := listener.syncCommitter.NewCommitRequest(topicreadercommon.CommitRange{
		PartitionSession:  secondSession,
		CommitOffsetStart: 10,
		CommitOffsetEnd:   11,
	})
	result := make(chan error, 1)
	go func() {
		result <- request.Wait(ctx)
	}()
	message := xtest.Receive(t, sent, "the replacement session commit")
	commit, ok := message.(*rawtopicreader.CommitOffsetRequest)
	require.True(t, ok)
	require.Len(t, commit.CommitOffsets, 1)
	require.Equal(t, secondSession.StreamPartitionSessionID, commit.CommitOffsets[0].PartitionSessionID)

	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.CommitOffsetResponse{
		PartitionsCommittedOffsets: []rawtopicreader.PartitionCommittedOffset{{
			PartitionSessionID: firstSession.StreamPartitionSessionID,
			CommittedOffset:    11,
		}},
	}))
	require.Equal(t, int64(11), firstSession.CommittedOffset().ToInt64())
	require.Equal(t, int64(10), secondSession.CommittedOffset().ToInt64(),
		"an old acknowledgement must not advance the replacement session")
	select {
	case err := <-result:
		t.Fatalf("replacement commit finished after another session's acknowledgement: %v", err)
	default:
	}

	require.NoError(t, listener.routeMessage(ctx, &rawtopicreader.CommitOffsetResponse{
		PartitionsCommittedOffsets: []rawtopicreader.PartitionCommittedOffset{{
			PartitionSessionID: secondSession.StreamPartitionSessionID,
			CommittedOffset:    11,
		}},
	}))
	require.NoError(t, xtest.Receive(t, result, "the replacement session's own acknowledgement"))
}
