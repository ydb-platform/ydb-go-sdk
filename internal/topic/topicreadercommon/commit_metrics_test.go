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

func TestCommitRangeMetadataPreservesLogicalMessageIdentity(t *testing.T) {
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
	require.Equal(t, []rawtopiccommon.Offset{10, 14}, GetCommitRange(batch).MessageOffsets())
	require.Equal(t, []rawtopiccommon.Offset{10}, GetCommitRange(batch.Messages[0]).MessageOffsets())
	offsets := GetCommitRange(batch).MessageOffsets()
	offsets[0] = 100
	require.Equal(t, []rawtopiccommon.Offset{10, 14}, GetCommitRange(batch).MessageOffsets())

	batch.Messages[0].Offset = 100
	batch.Messages[1] = &PublicMessage{Offset: 101}
	require.Equal(t, []rawtopiccommon.Offset{10}, GetCommitRange(batch.Messages[0]).MessageOffsets())
	require.Equal(t, []rawtopiccommon.Offset{10, 14}, GetCommitRange(batch).MessageOffsets())

	head, rest := BatchCutMessages(batch, 1)
	require.Equal(t, []rawtopiccommon.Offset{10}, GetCommitRange(head).MessageOffsets())
	require.Equal(t, []rawtopiccommon.Offset{14}, GetCommitRange(rest).MessageOffsets())

	ranges := CommitRanges{}
	ranges.AppendCommitRange(GetCommitRange(head))
	ranges.AppendCommitRange(GetCommitRange(rest))
	ranges.Optimize()
	require.Len(t, ranges.Ranges, 1)
	require.Equal(t, []rawtopiccommon.Offset{10, 14}, ranges.Ranges[0].MessageOffsets())

	merged, err := BatchAppend(head, rest)
	require.NoError(t, err)
	require.Equal(t, []rawtopiccommon.Offset{10, 14}, GetCommitRange(merged).MessageOffsets())
}

func TestCommitRangesOptimizeOwnsMetadataSnapshots(t *testing.T) {
	session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
	lhsMetadata := make([]commitMessageMetadata, 1, 4)
	lhsMetadata[0].offset = 10
	rhsMetadata := []commitMessageMetadata{{offset: 11}}
	lhs := CommitRange{
		CommitOffsetStart: 10,
		CommitOffsetEnd:   11,
		PartitionSession:  session,
		messageMetadata:   lhsMetadata,
	}
	rhs := CommitRange{
		CommitOffsetStart: 11,
		CommitOffsetEnd:   12,
		PartitionSession:  session,
		messageMetadata:   rhsMetadata,
	}

	snapshot := GetCommitRange(lhs)
	require.Equal(t, len(lhsMetadata), cap(snapshot.messageMetadata))

	ranges := CommitRanges{}
	ranges.AppendCommitRange(lhs)
	ranges.AppendCommitRange(rhs)
	ranges.Optimize()
	require.Equal(t, []rawtopiccommon.Offset{10, 11}, ranges.Ranges[0].MessageOffsets())

	// A later append to the source batch/range must not overwrite the
	// optimized snapshot's spare metadata slots.
	_ = appendCommitMessageMetadata(
		lhsMetadata,
		[]commitMessageMetadata{{offset: 99}},
		len(lhsMetadata),
		1,
	)
	require.Equal(t, []rawtopiccommon.Offset{10, 11}, ranges.Ranges[0].MessageOffsets())
}

func TestCommitRangesOptimizeIsSafeDuringSourceAppend(t *testing.T) {
	const (
		sourceCapacity = 1 << 16
		mergedMessages = 1 << 16
	)

	session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
	sourceMetadata := make([]commitMessageMetadata, 1, sourceCapacity)
	sourceMetadata[0].offset = 10
	mergedMetadata := make([]commitMessageMetadata, mergedMessages)
	for i := range mergedMetadata {
		mergedMetadata[i].offset = rawtopiccommon.Offset(11 + i)
	}

	ranges := CommitRanges{}
	ranges.AppendCommitRange(CommitRange{
		CommitOffsetStart: 10,
		CommitOffsetEnd:   11,
		PartitionSession:  session,
		messageMetadata:   sourceMetadata,
	})
	ranges.AppendCommitRange(CommitRange{
		CommitOffsetStart: 11,
		CommitOffsetEnd:   rawtopiccommon.Offset(11 + mergedMessages),
		PartitionSession:  session,
		messageMetadata:   mergedMetadata,
	})

	start := make(chan struct{})
	optimized := make(chan struct{})
	appended := make(chan struct{})
	go func() {
		<-start
		ranges.Optimize()
		close(optimized)
	}()
	go func() {
		<-start
		for i := range sourceCapacity - 1 {
			sourceMetadata = append(sourceMetadata, commitMessageMetadata{
				offset: rawtopiccommon.Offset(100000 + i),
			})
		}
		close(appended)
	}()
	close(start)
	<-optimized
	<-appended

	require.Len(t, ranges.Ranges, 1)
	require.Len(t, ranges.Ranges[0].MessageOffsets(), 1+mergedMessages)
}

func TestCommitRangeMetadataIsOptIn(t *testing.T) {
	session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
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
	require.Nil(t, GetCommitRange(batch).MessageOffsets())
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

func TestPublicMessageBuilderCapturesSingleMessageIdentity(t *testing.T) {
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	})

	message := NewPublicMessageBuilder().
		Offset(21).
		PartitionSession(session).
		Build()
	require.Equal(t, []rawtopiccommon.Offset{21}, GetCommitRange(message).MessageOffsets())

	message = NewPublicMessageBuilder().
		PartitionSession(session).
		Offset(22).
		Build()
	require.Equal(t, []rawtopiccommon.Offset{22}, GetCommitRange(message).MessageOffsets())
}

func TestCommitTraceUsesLogicalCountsAndDeduplicatesAcknowledgements(t *testing.T) {
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

	TraceCommitQueued(session.Context(), GetCommitRange(batch))
	require.Len(t, queued, 1)
	require.Equal(t, 2, queued[0].MessagesCount)
	require.Equal(t, "endpoint", queued[0].Endpoint)
	require.Equal(t, "database", queued[0].Database)
	require.Equal(t, "consumer", queued[0].Consumer)
	require.Equal(t, "reader", queued[0].ReaderName)
	require.Equal(t, int64(1), queued[0].PartitionID)
	require.Equal(t, int64(2), queued[0].PartitionSessionID)

	TraceCommitAcknowledged(session.Context(), session, 11)
	TraceCommitAcknowledged(session.Context(), session, 11)
	TraceCommitAcknowledged(session.Context(), session, 15)
	require.Len(t, acknowledged, 2)
	require.Equal(t, 1, acknowledged[0].MessagesCount)
	require.Equal(t, 1, acknowledged[1].MessagesCount)
	session.Close()
	TraceCommitQueued(session.Context(), GetCommitRange(batch))
	TraceCommitAcknowledged(session.Context(), session, 16)
	require.Len(t, queued, 1)
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

func TestCommitMessageTrackerCountsLogicalOffsets(t *testing.T) {
	tracker := NewCommitMessageTracker(0)

	require.Equal(t, 2, tracker.Queue([]rawtopiccommon.Offset{10, 14}))
	require.Equal(t, 1, tracker.Acknowledge(11))
	require.Equal(t, 1, tracker.Acknowledge(15))
}

func TestCommitMessageTrackerDeduplicatesPendingOffsets(t *testing.T) {
	tracker := NewCommitMessageTracker(0)

	require.Equal(t, 2, tracker.Queue([]rawtopiccommon.Offset{10, 11}))
	require.Equal(t, 3, tracker.Queue([]rawtopiccommon.Offset{10, 10, 11}))
	require.Equal(t, 2, tracker.Acknowledge(12))
	require.Zero(t, tracker.Acknowledge(12))
}

func TestCommitMessageTrackerIgnoresStaleAndUnknownAcknowledgements(t *testing.T) {
	tracker := NewCommitMessageTracker(0)

	require.Zero(t, tracker.Acknowledge(10))
	require.Equal(t, 3, tracker.Queue([]rawtopiccommon.Offset{10, 11, 14}))
	require.Equal(t, 2, tracker.Acknowledge(12))
	require.Zero(t, tracker.Acknowledge(11))
	require.Zero(t, tracker.Acknowledge(13))
	require.Equal(t, 1, tracker.Acknowledge(15))
	require.Zero(t, tracker.Acknowledge(100))
}

func TestCommitMessageTrackerSkipsOffsetsBelowWatermark(t *testing.T) {
	tracker := NewCommitMessageTracker(10)

	require.Equal(t, 4, tracker.Queue([]rawtopiccommon.Offset{1, 9, 10, 11}))
	require.Equal(t, 1, tracker.Acknowledge(11))
	require.Equal(t, 1, tracker.Acknowledge(100))
	require.Zero(t, tracker.Acknowledge(100))
	require.Equal(t, 3, tracker.Queue([]rawtopiccommon.Offset{1, 9, 10}))
	require.Zero(t, tracker.Acknowledge(101))
}

func TestCommitMessageTrackerKeepsIndependentLedgers(t *testing.T) {
	first := NewCommitMessageTracker(0)
	second := NewCommitMessageTracker(0)

	require.Equal(t, 1, first.Queue([]rawtopiccommon.Offset{10}))
	require.Equal(t, 1, second.Queue([]rawtopiccommon.Offset{10}))
	require.Equal(t, 1, first.Acknowledge(11))
	require.Equal(t, 1, second.Acknowledge(11))
}

func TestCommitMessageTrackerHandlesEmptyInput(t *testing.T) {
	tracker := NewCommitMessageTracker(0)

	require.Zero(t, tracker.Queue(nil))
	require.Zero(t, tracker.Queue([]rawtopiccommon.Offset{}))
	require.Zero(t, tracker.Acknowledge(0))
	require.Zero(t, tracker.Acknowledge(1))

	tracker.Close()
	require.Zero(t, tracker.Queue(nil))
	require.Zero(t, tracker.Acknowledge(2))
}

func TestCommitMessageTrackerCloseIsIdempotentAndTerminal(t *testing.T) {
	tracker := NewCommitMessageTracker(0)

	require.Equal(t, 1, tracker.Queue([]rawtopiccommon.Offset{10}))
	tracker.Close()
	tracker.Close()

	require.Zero(t, tracker.Queue([]rawtopiccommon.Offset{10, 11}))
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

			offsets := make([]rawtopiccommon.Offset, messagesPerWorker)
			for i := range offsets {
				offsets[i] = rawtopiccommon.Offset(worker*messagesPerWorker + i + 1)
			}
			queued.Add(int64(tracker.Queue(offsets)))
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
			lateQueued.Add(int64(tracker.Queue([]rawtopiccommon.Offset{1})))
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
	require.Zero(t, tracker.Queue([]rawtopiccommon.Offset{1}))
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
		ReaderName: "reader",
	})
	t.Cleanup(session.Close)

	return session
}
