package topicreadercommon

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
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

func TestCommitMetricsSetupAfterSessionCloseIsIgnored(t *testing.T) {
	queued := 0
	session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
	session.Close()
	session.SetupCommitMetrics(&trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {
			queued++
		},
	}, ReaderInfo{})
	message := NewPublicMessageBuilder().PartitionSession(session).Offset(10).Build()

	TraceCommitQueued(session.Context(), GetCommitRange(message))
	require.Zero(t, queued)
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
	var acknowledged []trace.TopicReaderCommitAcknowledgedInfo
	tracer := &trace.Topic{
		OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
			queued = append(queued, info)
		},
		OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
			acknowledged = append(acknowledged, info)
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
	require.Equal(t, "endpoint", queued[0].Endpoint)
	require.Equal(t, "database", queued[0].Database)
	require.Equal(t, "consumer", queued[0].Consumer)
	require.Equal(t, readerNamePointer("reader"), queued[0].ReaderName)
	require.Equal(t, int64(1), queued[0].PartitionID)
	require.Equal(t, int64(2), queued[0].PartitionSessionID)

	TraceCommitAcknowledged(session.Context(), session, 14)
	require.Empty(t, acknowledged)
	TraceCommitAcknowledged(session.Context(), session, 16)
	require.Len(t, acknowledged, 1)
	require.Equal(t, 12, acknowledged[0].MessagesCount)
	TraceCommitAcknowledged(session.Context(), session, 16)
	TraceCommitAcknowledged(session.Context(), session, 19)
	require.Len(t, acknowledged, 1)
	TraceCommitAcknowledged(session.Context(), session, 20)
	require.Len(t, acknowledged, 2)
	require.Equal(t, 6, acknowledged[1].MessagesCount)
	session.Close()
	TraceCommitQueued(session.Context(), first)
	TraceCommitAcknowledged(session.Context(), session, 16)
	require.Len(t, queued, 3)
	require.Len(t, acknowledged, 2)
}

func TestCommitQueuedRegistrationPrecedesTraceEmission(t *testing.T) {
	queued := 0
	acknowledged := 0
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {
			queued++
		},
		OnReaderCommitAcknowledged: func(trace.TopicReaderCommitAcknowledgedInfo) {
			acknowledged++
		},
	})
	message := NewPublicMessageBuilder().PartitionSession(session).Offset(10).Build()
	commitRange := GetCommitRange(message)

	messagesCount := RegisterCommitQueued(commitRange)
	require.Equal(t, 1, messagesCount)
	TraceCommitAcknowledged(session.Context(), session, 11)
	require.Equal(t, 1, acknowledged)
	require.Zero(t, queued)

	TraceCommitQueuedAfterRegistration(session.Context(), commitRange, messagesCount)
	require.Equal(t, 1, queued)
	TraceCommitAcknowledged(session.Context(), session, 11)
	require.Equal(t, 1, acknowledged)
}

func TestCommitAcknowledgedRegistrationSurvivesCloseBeforeTrace(t *testing.T) {
	acknowledged := make(chan int, 1)
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
			acknowledged <- info.MessagesCount
		},
	})
	message := NewPublicMessageBuilder().PartitionSession(session).Offset(10).Build()
	commitRange := GetCommitRange(message)

	require.Equal(t, 1, RegisterCommitQueued(commitRange))
	messagesCount := RegisterCommitAcknowledged(session, 11)
	require.Equal(t, 1, messagesCount)
	session.SetCommittedOffsetForward(11)
	session.Close()

	TraceCommitAcknowledgedAfterRegistration(session.Context(), session, messagesCount)
	select {
	case count := <-acknowledged:
		require.Equal(t, 1, count)
	case <-time.After(time.Second):
		t.Fatal("acknowledged event was lost after session close")
	}
}

func TestCommitterRegistersAcknowledgementBeforeWaiterNotification(t *testing.T) {
	allowTrace := make(chan struct{})
	acknowledged := make(chan int, 1)
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
			<-allowTrace
			acknowledged <- info.MessagesCount
		},
	})
	message := NewPublicMessageBuilder().PartitionSession(session).Offset(10).Build()
	committer := NewCommitterStopped(
		&trace.Topic{},
		context.Background(),
		CommitModeSync,
		func(rawtopicreader.ClientMessage) error { return nil },
	)
	t.Cleanup(func() {
		require.NoError(t, committer.Close(context.Background(), errors.New("test committer closed")))
	})

	waiter, err := committer.pushCommit(GetCommitRange(message))
	require.NoError(t, err)
	go func() {
		<-waiter.Committed
		session.Close()
		close(allowTrace)
	}()

	notifyDone := make(chan struct{})
	go func() {
		committer.OnCommitNotify(session, 11)
		close(notifyDone)
	}()
	select {
	case <-notifyDone:
	case <-time.After(time.Second):
		t.Fatal("commit notification did not complete")
	}
	select {
	case count := <-acknowledged:
		require.Equal(t, 1, count)
	case <-time.After(time.Second):
		t.Fatal("acknowledged event was lost after waiter notification")
	}
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

func TestCommitMessageTrackerKeepsIndependentLedgers(t *testing.T) {
	first := NewCommitMessageTracker(0)
	second := NewCommitMessageTracker(0)

	require.Equal(t, 1, first.Queue(10, 11))
	require.Equal(t, 1, second.Queue(10, 11))
	require.Equal(t, 1, first.Acknowledge(11))
	require.Equal(t, 1, second.Acknowledge(11))
}

func TestCommitMessageTrackerHandlesEmptyAndInvalidRanges(t *testing.T) {
	tracker := NewCommitMessageTracker(0)

	require.Zero(t, tracker.Queue(0, 0))
	require.Zero(t, tracker.Queue(2, 1))
	require.Zero(t, tracker.Acknowledge(0))
	require.Zero(t, tracker.Acknowledge(1))

	tracker.Close()
	require.Zero(t, tracker.Queue(1, 2))
	require.Zero(t, tracker.Acknowledge(2))
}

func TestCommitMessageTrackerCloseIsIdempotentAndTerminal(t *testing.T) {
	tracker := NewCommitMessageTracker(0)

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
	waitCommitMessageTrackerGroup(t, &queueWG)
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
	waitCommitMessageTrackerGroup(t, &acknowledgeWG)

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
	waitCommitMessageTrackerGroup(t, &lateWG)
	waitCommitMessageTrackerGroup(t, &closeWG)

	require.Equal(t, int64(workers*messagesPerWorker), acknowledged.Load())
	require.GreaterOrEqual(t, lateQueued.Load(), int64(0))
	require.LessOrEqual(t, lateQueued.Load(), int64(workers))
	require.Zero(t, lateAcknowledged.Load())
	require.Zero(t, tracker.Queue(1, 2))
	require.Zero(t, tracker.Acknowledge(workers*messagesPerWorker+2))
}

func waitCommitMessageTrackerGroup(t *testing.T, group *sync.WaitGroup) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("commit message tracker operation group did not complete")
	}
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
		ReaderName: readerNamePointer("reader"),
	})
	t.Cleanup(session.Close)

	return session
}
