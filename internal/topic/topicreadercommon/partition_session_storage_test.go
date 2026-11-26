package topicreadercommon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReleasePendingCommitsOnUntrackedStorageDoesNotAllocate(t *testing.T) {
	storage := &PartitionSessionStorage{}
	allocs := testing.AllocsPerRun(1000, func() {
		*storage = PartitionSessionStorage{}
		storage.ReleasePendingCommits()
	})
	t.Logf("first release: %g allocs/op", allocs)
	require.Zero(t, allocs)
}

func TestReleasePendingCommitsRejectsLateTrackerSubmissions(t *testing.T) {
	var storage PartitionSessionStorage
	storage.ReleasePendingCommits()
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
	})
	require.NoError(t, storage.Add(session))
	require.Zero(t, RegisterCommitQueued(CommitRange{PartitionSession: session, CommitOffsetEnd: 1}))
	require.Zero(t, RegisterCommitAcknowledged(session, 1))
}
