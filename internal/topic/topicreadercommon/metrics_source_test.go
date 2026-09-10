package topicreadercommon

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

func TestReaderMetricsSourceTracksOldestRetainedMessage(t *testing.T) {
	session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 1, 1, 0)
	t.Cleanup(session.Close)
	first := NewPublicMessageBuilder().PartitionSession(session).Offset(0).Build()
	second := NewPublicMessageBuilder().PartitionSession(session).Offset(1).Build()
	newest := NewPublicMessageBuilder().PartitionSession(session).Offset(2).Build()
	oldBatch, err := NewBatch(session, []*PublicMessage{first, second})
	require.NoError(t, err)
	newBatch, err := NewBatch(session, []*PublicMessage{newest})
	require.NoError(t, err)

	source := NewReaderMetricsSource()
	oldReceivedAt := time.Now().Add(-time.Minute)
	source.TrackBatch(oldBatch, oldReceivedAt)
	snapshot := source.Snapshot()
	require.GreaterOrEqual(t, snapshot.OldestMessageAge, 50*time.Second)
	initialAge := snapshot.OldestMessageAge
	time.Sleep(2 * time.Millisecond)
	require.Greater(t, source.Snapshot().OldestMessageAge, initialAge)

	source.TrackBatch(newBatch, time.Now())
	partial, err := NewBatch(session, []*PublicMessage{first})
	require.NoError(t, err)
	source.ReleaseBatch(partial)
	require.GreaterOrEqual(t, source.Snapshot().OldestMessageAge, 50*time.Second)

	remaining, err := NewBatch(session, []*PublicMessage{second})
	require.NoError(t, err)
	source.ReleaseBatch(remaining)
	snapshot = source.Snapshot()
	require.Less(t, snapshot.OldestMessageAge, time.Second)

	source.ReleaseBatch(newBatch)
	require.Zero(t, source.Snapshot().OldestMessageAge)
}

func TestReaderMetricsSourceRemovesReceiptMetadata(t *testing.T) {
	session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 1, 1, 0)
	t.Cleanup(session.Close)
	source := NewReaderMetricsSource()
	oldMessage := NewPublicMessageBuilder().PartitionSession(session).Offset(0).Build()
	oldBatch, err := NewBatch(session, []*PublicMessage{oldMessage})
	require.NoError(t, err)
	source.TrackBatch(oldBatch, time.Now().Add(-time.Minute))

	for offset := int64(1); offset <= 256; offset++ {
		message := NewPublicMessageBuilder().PartitionSession(session).Offset(offset).Build()
		batch, batchErr := NewBatch(session, []*PublicMessage{message})
		require.NoError(t, batchErr)
		receivedAt := time.Now().Add(time.Duration(offset) * time.Nanosecond)
		source.TrackBatch(batch, receivedAt)
		source.ReleaseBatch(batch)
	}

	source.mu.Lock()
	require.Len(t, source.receiptTimes, 1)
	require.Len(t, source.receiptEntries, 1)
	require.Len(t, source.receiptCounts, 1)
	source.mu.Unlock()

	source.ReleaseBatch(oldBatch)
	source.mu.Lock()
	require.Empty(t, source.receiptTimes)
	require.Empty(t, source.receiptEntries)
	require.Empty(t, source.receiptCounts)
	source.mu.Unlock()
}

func TestReaderMetricsSourceTracksMaximumCommitLagAndActiveSessions(t *testing.T) {
	source := NewReaderMetricsSource()
	first := NewPartitionSession(context.Background(), "topic-a", 1, 1, "", 1, 1, 0)
	second := NewPartitionSession(context.Background(), "topic-b", 2, 1, "", 2, 2, 10)
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)
	source.RegisterPartitionSession(first)
	source.RegisterPartitionSession(second)

	source.RegisterCommit(first, 15)
	source.RegisterCommit(first, 10)
	source.RegisterCommit(second, 30)
	source.AcknowledgeCommit(second, 25)
	source.AcknowledgeCommit(second, 20)
	snapshot := source.Snapshot()
	require.Equal(t, int64(2), snapshot.PartitionSessionCount)
	require.Equal(t, int64(15), snapshot.CommitOffsetLag)

	source.AcknowledgeCommit(first, 15)
	snapshot = source.Snapshot()
	require.Equal(t, int64(5), snapshot.CommitOffsetLag)

	source.UnregisterPartitionSession(second)
	snapshot = source.Snapshot()
	require.Equal(t, int64(1), snapshot.PartitionSessionCount)
	require.Zero(t, snapshot.CommitOffsetLag)
}

func TestReaderMetricsSourceAdvancesAcknowledgedOffsetForTransactions(t *testing.T) {
	source := NewReaderMetricsSource()
	session := NewPartitionSession(context.Background(), "topic", 1, 1, "", 1, 1, 0)
	t.Cleanup(session.Close)
	session.SetupMetricsSource(source)
	source.RegisterPartitionSession(session)

	session.SetCommittedOffsetForward(100)
	source.RegisterCommit(session, 110)
	require.Equal(t, int64(10), source.Snapshot().CommitOffsetLag)
}

func TestReaderMetricsSourceDoesNotResurrectClosedSession(t *testing.T) {
	source := NewReaderMetricsSource()
	for i := range 100 {
		session := NewPartitionSession(
			context.Background(), "topic", int64(i), 1, "", rawtopicreader.PartitionSessionID(i), int64(i), 0,
		)
		session.SetupMetricsSource(source)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			source.RegisterPartitionSession(session)
		}()
		go func() {
			defer wg.Done()
			session.Close()
		}()
		wg.Wait()
		require.Zero(t, source.Snapshot().PartitionSessionCount)
	}
}
