package topicreadercommon

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCommitRangeBoundariesSurviveBatchOperationsAndPublicMutation(t *testing.T) {
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	})
	batch, err := NewBatchFromStream(
		NewMultiDecoder(),
		session,
		rawtopicreader.Batch{
			Codec: rawtopiccommon.CodecRaw,
			MessageData: []rawtopicreader.MessageData{
				{Offset: 10},
				{Offset: 14},
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, rawtopiccommon.Offset(0), GetCommitRange(batch).CommitOffsetStart)
	require.Equal(t, rawtopiccommon.Offset(15), GetCommitRange(batch).CommitOffsetEnd)
	require.Equal(t, 15, RegisterCommitQueued(GetCommitRange(batch)))

	batch.Messages[0].Offset = 100
	batch.Messages[1].Offset = 101
	require.Equal(t, rawtopiccommon.Offset(0), GetCommitRange(batch).CommitOffsetStart)
	require.Equal(t, rawtopiccommon.Offset(15), GetCommitRange(batch).CommitOffsetEnd)
	originalMessages := batch.Messages
	batch.Messages = []*PublicMessage{originalMessages[1]}
	require.Equal(t, rawtopiccommon.Offset(0), GetCommitRange(batch).CommitOffsetStart)
	require.Equal(t, rawtopiccommon.Offset(15), GetCommitRange(batch).CommitOffsetEnd)
	batch.Messages = originalMessages

	head, rest := BatchCutMessages(batch, 1)
	require.Equal(t, rawtopiccommon.Offset(0), GetCommitRange(head).CommitOffsetStart)
	require.Equal(t, rawtopiccommon.Offset(11), GetCommitRange(head).CommitOffsetEnd)
	require.Equal(t, rawtopiccommon.Offset(11), GetCommitRange(rest).CommitOffsetStart)
	require.Equal(t, rawtopiccommon.Offset(15), GetCommitRange(rest).CommitOffsetEnd)

	ranges := CommitRanges{}
	ranges.AppendCommitRange(GetCommitRange(head))
	ranges.AppendCommitRange(GetCommitRange(rest))
	ranges.Optimize()
	require.Len(t, ranges.Ranges, 1)
	require.Equal(t, rawtopiccommon.Offset(0), ranges.Ranges[0].CommitOffsetStart)
	require.Equal(t, rawtopiccommon.Offset(15), ranges.Ranges[0].CommitOffsetEnd)

	merged, err := BatchAppend(head, rest)
	require.NoError(t, err)
	require.Equal(t, rawtopiccommon.Offset(0), GetCommitRange(merged).CommitOffsetStart)
	require.Equal(t, rawtopiccommon.Offset(15), GetCommitRange(merged).CommitOffsetEnd)
}

func TestSetupCommitMetricsRequiresQueuedTrace(t *testing.T) {
	var nilSession *PartitionSession
	nilSession.SetupCommitMetrics(&trace.Topic{}, ReaderInfo{})

	logOnlyTrace := log.Topic(log.Default(io.Discard), trace.DetailsAll)
	tests := []struct {
		name   string
		tracer *trace.Topic
	}{
		{name: "nil", tracer: nil},
		{name: "empty", tracer: &trace.Topic{}},
		{name: "log only old notify", tracer: &logOnlyTrace},
		{name: "composed empty", tracer: gtrace.Compose(&trace.Topic{}, &trace.Topic{})},
		{
			name:   "composed log only old notify",
			tracer: gtrace.Compose(&logOnlyTrace, &trace.Topic{}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
			defer session.Close()
			session.SetupCommitMetrics(test.tracer, ReaderInfo{})
			require.Nil(t, session.commitMetrics)
		})
	}

	// Queued tracing is the explicit opt-in that also enables ACK range tracking.
	queuedOnlySession := NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
	defer queuedOnlySession.Close()
	queuedOnlySession.SetupCommitMetrics(&trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	}, ReaderInfo{})
	require.NotNil(t, queuedOnlySession.commitMetrics)
	require.NotNil(t, queuedOnlySession.commitMetrics.tracker)

	closedSession := NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
	closedSession.Close()
	closedSession.SetupCommitMetrics(&trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {
			t.Fatal("closed session must not emit queued trace")
		},
	}, ReaderInfo{})
	TraceCommitQueued(closedSession.Context(), CommitRange{PartitionSession: closedSession})
	require.Nil(t, closedSession.commitMetrics)
}

func TestPublicMessageBuilderPreservesCommitRangeBoundaries(t *testing.T) {
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	})

	message := NewPublicMessageBuilder().
		CommitRange(CommitRange{
			CommitOffsetStart: 21,
			CommitOffsetEnd:   27,
			PartitionSession:  session,
		}).
		Build()
	require.Equal(t, rawtopiccommon.Offset(21), GetCommitRange(message).CommitOffsetStart)
	require.Equal(t, rawtopiccommon.Offset(27), GetCommitRange(message).CommitOffsetEnd)
	require.Equal(t, 6, RegisterCommitQueued(GetCommitRange(message)))
}

func TestCommitTraceUsesRangeCountsAndFIFOAcknowledgements(t *testing.T) {
	var queued []trace.TopicReaderCommitQueuedInfo
	tracer := &trace.Topic{
		OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
			queued = append(queued, info)
		},
	}
	session := newCommitMetricsTestSession(t, tracer)
	first := CommitRange{CommitOffsetStart: 10, CommitOffsetEnd: 16, PartitionSession: session}
	second := CommitRange{CommitOffsetStart: 10, CommitOffsetEnd: 16, PartitionSession: session}
	overlapping := CommitRange{CommitOffsetStart: 14, CommitOffsetEnd: 20, PartitionSession: session}

	TraceCommitQueued(session.Context(), first)
	TraceCommitQueued(session.Context(), second)
	TraceCommitQueued(session.Context(), overlapping)
	require.Len(t, queued, 3)
	require.Equal(t, 6, queued[0].MessagesCount)
	require.Equal(t, 6, queued[1].MessagesCount)
	require.Equal(t, 6, queued[2].MessagesCount)
	require.Equal(t, "endpoint", queued[0].ReaderInfo.Endpoint)
	require.Equal(t, "database", queued[0].ReaderInfo.Database)
	require.Equal(t, "consumer", queued[0].ReaderInfo.Consumer)
	require.Equal(t, "reader", queued[0].ReaderInfo.ReaderName)
	require.Equal(t, int64(1), queued[0].PartitionID)
	require.Equal(t, int64(2), queued[0].PartitionSessionID)

	require.Zero(t, RegisterCommitAcknowledged(session, 14))
	require.Equal(t, 12, RegisterCommitAcknowledged(session, 16))
	require.Zero(t, RegisterCommitAcknowledged(session, 16))
	require.Zero(t, RegisterCommitAcknowledged(session, 19))
	require.Equal(t, 6, RegisterCommitAcknowledged(session, 20))
	session.Close()
	TraceCommitQueued(session.Context(), first)
	require.Len(t, queued, 3)
	require.Zero(t, RegisterCommitAcknowledged(session, 20))
}

func TestQueuedRegistrationPrecedesTraceEmission(t *testing.T) {
	queued := 0
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {
			queued++
		},
	})
	message := NewPublicMessageBuilder().PartitionSession(session).Offset(10).Build()
	commitRange := GetCommitRange(message)

	messagesCount := RegisterCommitQueued(commitRange)
	require.Equal(t, 1, messagesCount)
	require.Equal(t, 1, RegisterCommitAcknowledged(session, 11))
	require.Zero(t, queued)

	TraceCommitQueuedAfterRegistration(session.Context(), commitRange, messagesCount)
	require.Equal(t, 1, queued)
	require.Zero(t, RegisterCommitAcknowledged(session, 11))
}

func TestCommitAcknowledgedRegistrationStopsAfterClose(t *testing.T) {
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	})
	message := NewPublicMessageBuilder().PartitionSession(session).Offset(10).Build()
	commitRange := GetCommitRange(message)

	require.Equal(t, 1, RegisterCommitQueued(commitRange))
	require.Equal(t, 1, RegisterCommitAcknowledged(session, 11))
	session.SetCommittedOffsetForward(11)
	session.Close()
	require.Zero(t, RegisterCommitAcknowledged(session, 12))
}

func TestCommitTraceCallbacksCanCloseSession(t *testing.T) {
	var session *PartitionSession
	queued := 0
	tracer := &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {
			queued++
			session.Close()
		},
	}
	session = newCommitMetricsTestSession(t, tracer)
	message := NewPublicMessageBuilder().PartitionSession(session).Offset(10).Build()

	TraceCommitQueued(session.Context(), GetCommitRange(message))
	require.Equal(t, 1, queued)
	TraceCommitQueued(session.Context(), GetCommitRange(message))
	require.Equal(t, 1, queued)
}

func TestCommitterTracesOnlyAcceptedCommits(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		var queued int
		session := newCommitMetricsTestSession(t, &trace.Topic{
			OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {
				queued++
			},
		})
		message := NewPublicMessageBuilder().PartitionSession(session).Offset(10).Build()
		committer := NewCommitterStopped(
			&trace.Topic{}, context.Background(), CommitModeAsync, func(rawtopicreader.ClientMessage) error {
				return nil
			})
		committer.Start()
		t.Cleanup(func() {
			require.NoError(t, committer.Close(context.Background(), errors.New("test committer closed")))
		})

		_, err := committer.pushCommit(GetCommitRange(message))
		require.NoError(t, err)
		require.Equal(t, 1, queued)
	})

	t.Run("closed", func(t *testing.T) {
		var queued int
		session := newCommitMetricsTestSession(t, &trace.Topic{
			OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {
				queued++
			},
		})
		message := NewPublicMessageBuilder().PartitionSession(session).Offset(10).Build()
		committer := NewCommitterStopped(
			&trace.Topic{}, context.Background(), CommitModeAsync, func(rawtopicreader.ClientMessage) error {
				return nil
			})
		committer.Start()
		require.NoError(t, committer.Close(context.Background(), errors.New("test committer closed")))

		_, err := committer.pushCommit(GetCommitRange(message))
		require.Error(t, err)
		require.Zero(t, queued)
	})

	t.Run("cancelled", func(t *testing.T) {
		var lifeContext context.Context
		lifeContext, cancel := context.WithCancel(context.Background())
		cancel()
		var queued int
		session := newCommitMetricsTestSession(t, &trace.Topic{
			OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {
				queued++
			},
		})
		message := NewPublicMessageBuilder().PartitionSession(session).Offset(10).Build()
		committer := NewCommitterStopped(
			&trace.Topic{}, lifeContext, CommitModeAsync, func(rawtopicreader.ClientMessage) error {
				return nil
			})

		_, err := committer.pushCommit(GetCommitRange(message))
		require.Error(t, err)
		require.Zero(t, queued)
		require.NoError(t, committer.Close(context.Background(), errors.New("test committer cancelled")))
	})
}

func TestCommitterQueueTraceCanReenterLazyRequest(t *testing.T) {
	ctx := context.Background()
	queued := make(chan trace.TopicReaderCommitQueuedInfo, 4)
	reentrantWait := make(chan error, 1)
	var request *commitRequest
	var committer *Committer
	var sends atomic.Int32
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
			queued <- info
			request.Confirm()
			reentrantWait <- request.Wait(ctx)
		},
	})
	committer = NewCommitterStopped(&trace.Topic{}, ctx, CommitModeSync,
		func(rawtopicreader.ClientMessage) error {
			sends.Add(1)
			committer.OnCommitNotify(session, 1)
			session.SetCommittedOffsetForward(1)

			return nil
		})
	committer.Start()
	t.Cleanup(func() {
		require.NoError(t, committer.Close(ctx, errors.New("test committer closed")))
	})
	request = committer.NewCommitRequest(CommitRange{
		PartitionSession: session, CommitOffsetStart: 0, CommitOffsetEnd: 1,
	})
	confirmDone := make(chan struct{})
	go func() {
		request.Confirm()
		close(confirmDone)
	}()

	require.NoError(t, xtest.Receive(t, reentrantWait, "reentrant commit wait"))
	_ = xtest.Receive(t, confirmDone, "commit confirm")
	for range 2 {
		request.Confirm()
		require.NoError(t, request.Wait(ctx))
	}
	require.Equal(t, int32(1), sends.Load())
	queuedInfo := xtest.Receive(t, queued, "queued commit metric")
	require.Empty(t, queued)
	require.Equal(t, 1, queuedInfo.MessagesCount)
}

func TestCommitMessageTrackerCountsRangeLengths(t *testing.T) {
	tracker := NewCommitMessageTracker(10)

	// An already completed range still reports its accepted span, but it is not
	// retained for a later acknowledgement.
	require.Equal(t, 9, tracker.Queue(0, 9))
	require.Zero(t, tracker.Acknowledge(10))

	require.Equal(t, 4, tracker.Queue(10, 14))
	require.Equal(t, 4, tracker.Queue(10, 14))
	require.Zero(t, tracker.Acknowledge(12))
	require.Equal(t, 8, tracker.Acknowledge(14))
	require.Zero(t, tracker.Acknowledge(14))
	require.Zero(t, tracker.Acknowledge(13))
	require.Equal(t, 3, tracker.Queue(10, 13))
	require.Zero(t, tracker.Acknowledge(14))
	require.Equal(t, 4, tracker.Queue(10, 14))
	require.Equal(t, 4, tracker.Acknowledge(14))
}

func TestCommitMessageTrackerHandlesFIFOAndEqualWatermark(t *testing.T) {
	tracker := NewCommitMessageTracker(10)

	require.Equal(t, 4, tracker.Queue(10, 14))
	require.Equal(t, 4, tracker.Acknowledge(14))
	// An end equal to the current watermark is admitted and completed by an
	// equal acknowledgement, as in the .NET request ledger.
	require.Equal(t, 4, tracker.Queue(10, 14))
	require.Equal(t, 4, tracker.Acknowledge(14))
	require.Equal(t, 4, tracker.Queue(14, 18))
	require.Zero(t, tracker.Acknowledge(14))
	require.Equal(t, 4, tracker.Acknowledge(18))

	// FIFO completion deliberately preserves head-of-line behavior for ranges
	// admitted out of offset order.
	tracker = NewCommitMessageTracker(0)
	require.Equal(t, 10, tracker.Queue(10, 20))
	require.Equal(t, 10, tracker.Queue(0, 10))
	require.Zero(t, tracker.Acknowledge(10))
	require.Equal(t, 20, tracker.Acknowledge(20))
}

func TestCommitMessageTrackerRejectsInvalidRangesAndCloses(t *testing.T) {
	tracker := NewCommitMessageTracker(0)

	require.Zero(t, tracker.Queue(0, 0))
	require.Zero(t, tracker.Queue(2, 1))
	require.Zero(t, tracker.Acknowledge(0))
	require.Zero(t, tracker.Acknowledge(1))
	require.Equal(t, 1, tracker.Queue(10, 11))
	tracker.Close()
	tracker.Close()

	require.Zero(t, tracker.Queue(10, 12))
	require.Zero(t, tracker.Acknowledge(12))
}

func TestCommitMessageTrackerConcurrentQueueAcknowledgeClose(t *testing.T) {
	const (
		workers           = 8
		messagesPerWorker = 64
	)

	tracker := NewCommitMessageTracker(0)
	start := make(chan struct{})
	var queueWG sync.WaitGroup
	var queued atomic.Int64
	queueWG.Add(workers)
	for worker := range workers {
		go func() {
			defer queueWG.Done()
			<-start

			for i := range messagesPerWorker {
				startOffset := rawtopiccommon.Offset(worker*messagesPerWorker + i + 1)
				queued.Add(int64(tracker.Queue(startOffset, startOffset+1)))
			}
		}()
	}
	close(start)
	xtest.WaitGroup(t, &queueWG)
	require.Equal(t, int64(workers*messagesPerWorker), queued.Load())

	var acknowledgeWG sync.WaitGroup
	var acknowledged atomic.Int64
	acknowledgeWG.Add(workers)
	for range workers {
		go func() {
			defer acknowledgeWG.Done()
			acknowledged.Add(int64(tracker.Acknowledge(workers*messagesPerWorker + 1)))
		}()
	}
	xtest.WaitGroup(t, &acknowledgeWG)

	var closeWG sync.WaitGroup
	var lateQueued atomic.Int64
	var lateAcknowledged atomic.Int64
	var lateWG sync.WaitGroup
	lateStart := make(chan struct{})
	lateWG.Add(workers * 2)
	for range workers {
		go func() {
			defer lateWG.Done()
			<-lateStart
			lateQueued.Add(int64(tracker.Queue(1, 2)))
		}()
	}
	for range workers {
		go func() {
			defer lateWG.Done()
			<-lateStart
			lateAcknowledged.Add(int64(tracker.Acknowledge(workers*messagesPerWorker + 2)))
		}()
	}
	closeWG.Add(1)
	go func() {
		defer closeWG.Done()
		<-lateStart
		tracker.Close()
	}()
	close(lateStart)
	xtest.WaitGroup(t, &lateWG)
	xtest.WaitGroup(t, &closeWG)

	require.Equal(t, int64(workers*messagesPerWorker), acknowledged.Load())
	require.GreaterOrEqual(t, lateQueued.Load(), int64(0))
	require.LessOrEqual(t, lateQueued.Load(), int64(workers))
	require.Zero(t, lateAcknowledged.Load())
	require.Zero(t, tracker.Queue(1, 2))
	require.Zero(t, tracker.Acknowledge(workers*messagesPerWorker+2))
}

func newCommitMetricsTestSession(t testing.TB, tracer *trace.Topic) *PartitionSession {
	t.Helper()

	session := NewPartitionSession(
		context.Background(),
		"topic",
		1,
		2,
		"connection",
		2,
		3,
		0,
	)
	session.SetupCommitMetrics(tracer, ReaderInfo{
		Endpoint:   "endpoint",
		Database:   "database",
		Consumer:   "consumer",
		ReaderName: "reader",
	})
	t.Cleanup(session.Close)

	return session
}
