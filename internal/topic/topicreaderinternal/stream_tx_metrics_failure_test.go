package topicreaderinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicStreamReader_PopMessagesBatchTxReleasesBufferWhenMaterializationFails(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	localDeltas := make(chan int, 4)
	received := make(chan int, 1)
	delivered := make(chan int, 1)
	queued := make(chan int, 1)
	acknowledged := make(chan int, 1)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			localDeltas <- info.MessagesDelta
		},
		OnReaderMessagesReceived: func(info trace.TopicReaderMessagesReceivedInfo) {
			received <- info.MessagesCount
		},
		OnReaderMessagesDelivered: func(info trace.TopicReaderMessagesDeliveredInfo) {
			delivered <- info.MessagesCount
		},
		OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
			queued <- info.MessagesCount
		},
		OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
			acknowledged <- info.MessagesCount
		},
	}

	// The stopped-reader fixture starts with one free-bytes token. Drain it so
	// ReadMessageBatch can return the owned message without a background worker.
	<-e.reader.freeBytes
	require.NoError(t, e.reader.onReadResponse(readerMetricResponse(&e, 50)))
	require.Equal(t, 1, readerMetricDelta(t, localDeltas))
	require.Equal(t, 1, readerMetricDelta(t, received))

	materializeErr := errors.New("materialize transaction failed")
	reader := newMetricsReader(&e)
	batch, err := reader.PopBatchTx(
		e.ctx,
		&failingMaterializeTransaction{
			mockTransaction: newMockTransactionWrapper("session", "transaction"),
			err:             materializeErr,
		},
	)

	require.Nil(t, batch)
	require.ErrorIs(t, err, materializeErr)
	require.ErrorContains(t, err, "failed to materialize transaction")
	require.Equal(t, -1, readerMetricDelta(t, localDeltas))
	readerMetricNoDelta(t, localDeltas)
	readerMetricNoDelta(t, delivered)
	readerMetricNoDelta(t, queued)
	readerMetricNoDelta(t, acknowledged)

	// Closing after the failed pop must not release the same ownership twice.
	require.NoError(t, reader.Close(context.Background()))
	readerMetricNoDelta(t, localDeltas)
}

type failingMaterializeTransaction struct {
	*mockTransaction

	err error
}

func (tx *failingMaterializeTransaction) UnLazy(context.Context) error {
	return tx.err
}
