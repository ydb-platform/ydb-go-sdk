package topicreaderinternal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicStreamReader_ReleaseLocalBufferForBatchIgnoresEmptyBatches(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	deltas := make(chan int, 1)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			deltas <- info.MessagesDelta
		},
	}

	e.reader.releaseLocalBufferForBatch(nil)
	e.reader.releaseLocalBufferForBatch(&topicreadercommon.PublicBatch{})
	readerMetricNoDelta(t, deltas)
}

func TestTopicStreamReader_DiscardBatchesUsesPartitionDrainBoundary(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	deltas := make(chan int, 4)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			deltas <- info.MessagesDelta
		},
	}

	batch := mustNewBatch(e.partitionSession, []*topicreadercommon.PublicMessage{{Offset: 0}})
	require.True(t, e.reader.reserveLocalBuffer(e.partitionSession.Topic, len(batch.Messages)))
	require.Equal(t, 1, readerMetricDelta(t, deltas))
	stopRequest := &rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: e.partitionSessionID,
	}
	e.reader.discardBatches([]batcherMessageOrderItem{newBatcherItemRawMessage(stopRequest)})
	readerMetricNoDelta(t, deltas)

	require.NoError(t, e.reader.batcher.PushBatches(batch))
	require.NoError(t, e.reader.batcher.PushRawMessage(e.partitionSession, stopRequest))
	e.reader.batcher.FlushPartitionSession(e.partitionSession)

	require.NoError(t, e.reader.onStopPartitionSessionRequestFromBuffer(stopRequest))
	require.Equal(t, -1, readerMetricDelta(t, deltas))
	e.reader.batcher.m.WithLock(func() {
		require.Empty(t, e.reader.batcher.messages)
		require.Empty(t, e.reader.batcher.sessionsForFlush)
	})
}

func TestReaderReconnectorSuppressesNilSessionError(t *testing.T) {
	require.True(t, suppressReaderSessionError(context.Background(), nil))
}
