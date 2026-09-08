package topicreaderinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicStreamReader_LocalBufferGuardsAndPartialRelease(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	deltas := make(chan int, 8)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			deltas <- info.MessagesDelta
		},
	}

	const topic = "/topic"
	// Releasing before the first reservation is a no-op because there is no map yet.
	e.reader.releaseLocalBuffer(topic, 1)
	e.reader.releaseLocalBuffer(topic, 0)

	require.True(t, e.reader.reserveLocalBuffer(topic, 3))
	require.Equal(t, 3, readerMetricDelta(t, deltas))

	// Unknown topics and releases larger than the balance must not change ownership.
	e.reader.releaseLocalBuffer("/other-topic", 1)
	e.reader.releaseLocalBuffer(topic, 4)
	readerMetricNoDelta(t, deltas)

	e.reader.releaseLocalBuffer(topic, 1)
	require.Equal(t, -1, readerMetricDelta(t, deltas))
	e.reader.finalizeLocalBuffer()
	require.Equal(t, -2, readerMetricDelta(t, deltas))

	// Finalization is terminal and idempotent for both reservations and releases.
	require.False(t, e.reader.reserveLocalBuffer(topic, 1))
	e.reader.releaseLocalBuffer(topic, 1)
	e.reader.finalizeLocalBuffer()
	readerMetricNoDelta(t, deltas)
}

func TestTopicStreamReader_LocalBufferFinalizeWithoutOutstandingMessages(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)
		deltas := make(chan int, 2)
		e.reader.cfg.Trace = &trace.Topic{
			OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
				deltas <- info.MessagesDelta
			},
		}

		e.reader.finalizeLocalBuffer()
		readerMetricNoDelta(t, deltas)
	})

	t.Run("fully released", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)
		deltas := make(chan int, 2)
		e.reader.cfg.Trace = &trace.Topic{
			OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
				deltas <- info.MessagesDelta
			},
		}

		require.True(t, e.reader.reserveLocalBuffer("/topic", 2))
		require.Equal(t, 2, readerMetricDelta(t, deltas))
		e.reader.releaseLocalBuffer("/topic", 2)
		require.Equal(t, -2, readerMetricDelta(t, deltas))
		e.reader.finalizeLocalBuffer()
		readerMetricNoDelta(t, deltas)
	})
}

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
	require.NoError(t, e.reader.batcher.PushBatches(batch))
	require.NoError(t, e.reader.batcher.PushRawMessage(e.partitionSession, &rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: e.partitionSessionID,
	}))
	e.reader.batcher.FlushPartitionSession(e.partitionSession)

	require.NoError(t, e.reader.onStopPartitionSessionRequestFromBuffer(&rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: e.partitionSessionID,
	}))
	require.Equal(t, -1, readerMetricDelta(t, deltas))
	e.reader.batcher.m.WithLock(func() {
		require.Empty(t, e.reader.batcher.messages)
		require.Empty(t, e.reader.batcher.sessionsForFlush)
	})
}

func TestTopicStreamReader_DiscardBatchesIgnoresRawItems(t *testing.T) {
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
	rawItem := newBatcherItemRawMessage(&rawtopicreader.StopPartitionSessionRequest{
		PartitionSessionID: e.partitionSessionID,
	})

	// Raw control messages do not own local-buffer credits.
	e.reader.discardBatches([]batcherMessageOrderItem{rawItem})
	readerMetricNoDelta(t, deltas)

	e.reader.discardBatches([]batcherMessageOrderItem{rawItem, newBatcherItemBatch(batch)})
	require.Equal(t, -1, readerMetricDelta(t, deltas))
}

func TestTopicStreamReader_CreditBalanceFinalizeIsIdempotent(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	deltas := make(chan int, 4)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
			deltas <- info.BytesDelta
		},
	}

	e.reader.changeCreditBalance(10)
	require.Equal(t, 10, readerMetricDelta(t, deltas))
	e.reader.finalizeCreditBalance()
	require.Equal(t, -10, readerMetricDelta(t, deltas))
	e.reader.finalizeCreditBalance()
	e.reader.changeCreditBalance(1)
	readerMetricNoDelta(t, deltas)
}

func TestReaderReconnectorSessionErrorNoOpStopAndNilError(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 1)
	reconnector := &readerReconnector{
		tracer:     sessionErrorTestTracer(events),
		readerInfo: sessionErrorTestReaderInfo(),
	}
	reconnector.initChannelsAndClock()

	reconnector.traceSessionStopIf(context.Background(), errors.New("stale reconnect request"), false)
	require.True(t, suppressReaderSessionError(context.Background(), nil))

	select {
	case event := <-events:
		t.Fatalf("unexpected session error event: %+v", event)
	default:
	}
}
