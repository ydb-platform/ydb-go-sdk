package topicreaderinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicStreamReaderMetricsSourceTracksPullSplitDiscardAndClose(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	source := topicreadercommon.NewReaderMetricsSource()
	e.reader.cfg.MetricsSource = source
	e.partitionSession.SetupMetricsSource(source)
	source.RegisterPartitionSession(e.partitionSession)

	require.NoError(t, e.reader.onReadResponse(readerMetricResponseWithOffsets(&e, 50, 20, 21, 22)))
	require.Positive(t, source.Snapshot().OldestMessageAge)

	first, err := e.reader.consumeMessagesUntilBatch(e.ctx, ReadMessageBatchOptions{
		batcherGetOptions: batcherGetOptions{MinCount: 1, MaxCount: 1},
	})
	require.NoError(t, err)
	require.Len(t, first.Messages, 1)
	e.reader.releaseLocalBufferForBatch(first)
	require.Positive(t, source.Snapshot().OldestMessageAge)

	remaining, err := e.reader.consumeMessagesUntilBatch(e.ctx, ReadMessageBatchOptions{
		batcherGetOptions: batcherGetOptions{MinCount: 1, MaxCount: 1},
	})
	require.NoError(t, err)
	require.Len(t, remaining.Messages, 1)
	e.reader.releaseLocalBufferForBatch(remaining)
	last, err := e.reader.consumeMessagesUntilBatch(e.ctx, ReadMessageBatchOptions{
		batcherGetOptions: batcherGetOptions{MinCount: 1, MaxCount: 1},
	})
	require.NoError(t, err)
	require.Len(t, last.Messages, 1)
	e.reader.releaseLocalBufferForBatch(last)
	require.Zero(t, source.Snapshot().OldestMessageAge)

	require.NoError(t, e.reader.onReadResponse(readerMetricResponseWithOffsets(&e, 50, 23, 24)))
	e.reader.discardBatches(e.reader.batcher.Drain())
	require.Zero(t, source.Snapshot().OldestMessageAge)

	require.Equal(t, int64(1), source.Snapshot().PartitionSessionCount)
	require.NoError(t, e.reader.CloseWithError(context.Background(), errors.New("test close")))
	require.Zero(t, source.Snapshot().PartitionSessionCount)
}

func TestReaderReconnectorMetricsSourceClosesOnlyOnTerminalStreamError(t *testing.T) {
	testCase := func(t *testing.T, ctx context.Context, streamErr error, wantClose bool) {
		t.Helper()
		closeCount := 0
		reconnector := &readerReconnector{
			retrySettings: topic.RetrySettings{},
			tracer:        &trace.Topic{},
			metricsSourceDone: func() {
				closeCount++
			},
		}
		reconnector.metricsSource = topicreadercommon.NewReaderMetricsSource()
		stream := &metricsSourceTestStream{err: streamErr}

		read := func(context.Context, batchedStreamReader) (*topicreadercommon.PublicBatch, error) {
			return nil, streamErr
		}
		_, _, _ = reconnector.readOnce(ctx, stream, read)
		_, _, _ = reconnector.readOnce(ctx, stream, read)
		if wantClose {
			require.Equal(t, 1, closeCount)
		} else {
			require.Zero(t, closeCount)
		}
	}

	testCase(t, context.Background(), errors.New("permission denied"), true)
	testCase(t, context.Background(), xerrors.Retryable(errors.New("temporary")), false)
	testCase(t, context.Background(), xerrors.WithStackTrace(errReconnect), false)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	testCase(t, canceled, errors.New("permission denied"), false)
}

type metricsSourceTestStream struct {
	err error
}

func (s *metricsSourceTestStream) WaitInit(context.Context) error { return nil }

func (s *metricsSourceTestStream) ReadMessageBatch(
	context.Context,
	ReadMessageBatchOptions,
) (*topicreadercommon.PublicBatch, error) {
	return nil, s.err
}

func (s *metricsSourceTestStream) Commit(context.Context, topicreadercommon.CommitRange) error {
	return nil
}

func (s *metricsSourceTestStream) CloseWithError(context.Context, error) error { return nil }

func (s *metricsSourceTestStream) PopMessagesBatchTx(
	context.Context,
	tx.Transaction,
	ReadMessageBatchOptions,
) (*topicreadercommon.PublicBatch, error) {
	return nil, s.err
}

func (*metricsSourceTestStream) TopicOnReaderStart(string, error) {}

func (s *metricsSourceTestStream) streamError() error { return s.err }
