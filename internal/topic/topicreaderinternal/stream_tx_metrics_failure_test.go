package topicreaderinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicStreamReader_PopMessagesBatchTxReleasesBufferWhenMaterializationFails(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	localDeltas := make(chan int, 4)
	received := make(chan int, 1)
	delivered := make(chan int, 1)
	queued := make(chan int, 1)
	acknowledged := make(chan int, 1)
	sessionErrors := make(chan trace.TopicReaderSessionErrorInfo, 4)
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
		OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
			sessionErrors <- info
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
	reconnector := reader.reader.(*readerReconnector)
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
	select {
	case event := <-sessionErrors:
		t.Fatalf("unexpected session error after transaction materialization failure: %+v", event)
	default:
	}
	require.False(t, reconnector.stopSessionErrorReported.Load())

	// The failed transactional pop has returned its message buffer ownership.
	// Drain that token so the next ordinary read can return another batch.
	<-e.reader.freeBytes
	require.NoError(t, e.reader.onReadResponse(readerMetricResponse(&e, 50)))
	batch, err = reader.ReadMessageBatch(e.ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, 1, readerMetricDelta(t, localDeltas))
	require.Equal(t, -1, readerMetricDelta(t, localDeltas))
	require.Equal(t, 1, readerMetricDelta(t, received))
	require.Equal(t, 1, readerMetricDelta(t, delivered))
	select {
	case event := <-sessionErrors:
		t.Fatalf("unexpected session error after successful ordinary read: %+v", event)
	default:
	}

	// Closing after the failed pop must not release the same ownership twice.
	require.NoError(t, reader.Close(context.Background()))
	readerMetricNoDelta(t, localDeltas)
	select {
	case event := <-sessionErrors:
		t.Fatalf("unexpected duplicate terminal stream session error after close: %+v", event)
	default:
	}
}

func TestTopicStreamReader_PopMessagesBatchTxHandlesUnLazyStreamFailure(t *testing.T) {
	tests := []struct {
		name      string
		streamErr string
		retryable bool
	}{
		{name: "retryable suppressed", streamErr: "retryable stream failure", retryable: true},
		{name: "terminal reported once", streamErr: "terminal stream failure"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := newTopicReaderTestEnv(t)
			sessionErrors := make(chan trace.TopicReaderSessionErrorInfo, 4)
			e.reader.cfg.Trace = &trace.Topic{
				OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
					sessionErrors <- info
				},
			}

			<-e.reader.freeBytes
			require.NoError(t, e.reader.onReadResponse(readerMetricResponse(&e, 50)))

			materializeErr := errors.New("materialize transaction failed")
			streamErr := errors.New(test.streamErr)
			if test.retryable {
				streamErr = xerrors.Retryable(streamErr)
			}
			reader := newMetricsReader(&e)
			reconnector := reader.reader.(*readerReconnector)
			batch, err := reader.PopBatchTx(
				e.ctx,
				&failingMaterializeTransaction{
					mockTransaction: newMockTransactionWrapper("session", "transaction"),
					err:             materializeErr,
					onUnLazy: func() {
						require.NoError(t, e.reader.CloseWithError(e.ctx, streamErr))
					},
				},
			)

			require.Nil(t, batch)
			require.ErrorIs(t, err, materializeErr)
			if test.retryable {
				select {
				case event := <-sessionErrors:
					t.Fatalf("unexpected session error for retryable stream failure: %+v", event)
				default:
				}
				require.False(t, reconnector.stopSessionErrorReported.Load())

				return
			}

			select {
			case event := <-sessionErrors:
				require.Equal(t, "stop", event.RetryDecision)
				require.ErrorIs(t, event.Error, streamErr)
				require.NotErrorIs(t, event.Error, materializeErr)
			case <-time.After(time.Second):
				t.Fatal("terminal stream session error was not emitted")
			}

			for range 2 {
				_, readErr := reader.ReadMessageBatch(e.ctx)
				require.ErrorIs(t, readErr, streamErr)
			}
			select {
			case event := <-sessionErrors:
				t.Fatalf("unexpected duplicate terminal stream session error: %+v", event)
			default:
			}
		})
	}
}

type failingMaterializeTransaction struct {
	*mockTransaction

	err      error
	onUnLazy func()
}

func (tx *failingMaterializeTransaction) UnLazy(context.Context) error {
	if tx.onUnLazy != nil {
		tx.onUnLazy()
	}

	return tx.err
}
