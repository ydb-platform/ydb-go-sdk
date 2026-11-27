package topiclistenerinternal

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestClosedListenerDoesNotRetainPendingCommits(t *testing.T) {
	control := retainedListenerCommitHeap(t, 0)
	pending := retainedListenerCommitHeap(t, 100_000)
	t.Logf("retained bytes: control=%d pending=%d difference=%d", control, pending, pending-control)
	require.Less(t, pending-control, int64(256<<10))
}

func retainedListenerCommitHeap(t *testing.T, count rawtopiccommon.Offset) int64 {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var message *topicreadercommon.PublicMessage
	t.Run("close stream", func(t *testing.T) {
		e := fixenv.New(t)
		listener := StreamListener(e)
		listener.cfg.ReaderInfo.Listener = true
		listener.tracer = &trace.Topic{OnListenerCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {}}
		started := make(chan struct{})
		EventHandlerMock(e).EXPECT().OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *PublicEventStartPartitionSession) error {
				close(started)

				return nil
			})
		partition := rawtopicreader.PartitionSession{PartitionSessionID: 42, Path: "/orders", PartitionID: 1}
		request := &rawtopicreader.StartPartitionSessionRequest{PartitionSession: partition}
		require.NoError(t, listener.handleStartPartition(listener.background.Context(), request))
		xtest.WaitChannelClosed(t, started)
		session, err := listener.sessions.Get(partition.PartitionSessionID)
		require.NoError(t, err)
		message = topicreadercommon.NewPublicMessageBuilder().PartitionSession(session).Offset(0).Build()
		for i := range count {
			topicreadercommon.RegisterCommitQueued(topicreadercommon.CommitRange{
				PartitionSession: session, CommitOffsetStart: i, CommitOffsetEnd: i + 1,
			})
		}
		require.NoError(t, listener.Close(xtest.Context(t), errors.New("connection lost before ACK")))
		require.ErrorIs(t, session.Context().Err(), context.Canceled)
	})
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(message)

	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}
