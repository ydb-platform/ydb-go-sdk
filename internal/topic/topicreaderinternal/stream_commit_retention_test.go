package topicreaderinternal

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestClosedReaderDoesNotRetainPendingCommits(t *testing.T) {
	control := retainedReaderCommitHeap(t, 0)
	pending := retainedReaderCommitHeap(t, 100_000)
	t.Logf("retained bytes: control=%d pending=%d difference=%d", control, pending, pending-control)
	require.Less(t, pending-control, int64(256<<10))
}

func retainedReaderCommitHeap(t *testing.T, count rawtopiccommon.Offset) int64 {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var message *topicreadercommon.PublicMessage
	t.Run("close stream", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)
		e.reader.cfg.Trace = &trace.Topic{OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {}}
		e.reader.startTrace()
		partition := rawtopicreader.PartitionSession{PartitionSessionID: 42, Path: "/orders", PartitionID: 1}
		require.NoError(t, e.reader.onStartPartitionSessionRequest(&rawtopicreader.StartPartitionSessionRequest{
			PartitionSession: partition,
		}))
		session, err := e.reader.sessionController.Get(partition.PartitionSessionID)
		require.NoError(t, err)
		message = topicreadercommon.NewPublicMessageBuilder().PartitionSession(session).Offset(0).Build()
		for i := range count {
			topicreadercommon.RegisterCommitQueued(topicreadercommon.CommitRange{
				PartitionSession: session, CommitOffsetStart: i, CommitOffsetEnd: i + 1,
			})
		}
		e.reader.backgroundWorkers.Start("readMessagesLoop", e.reader.readMessagesLoop)
		require.NoError(t, e.reader.CloseWithError(e.ctx, errors.New("connection lost before ACK")))
		require.ErrorIs(t, session.Context().Err(), context.Canceled)
	})
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(message)

	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}
