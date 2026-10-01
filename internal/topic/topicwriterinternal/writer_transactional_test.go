package topicwriterinternal

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTransactionalWriterReturnsConnectErrorWithoutRetry(t *testing.T) {
	want := xerrors.Retryable(errors.New("connection failed"))
	var connects atomic.Int32
	cfg := NewWriterReconnectorConfig(
		WithTransactionMode(),
		WithConnectFunc(func(context.Context, *trace.Topic) (RawTopicWriterStream, error) {
			connects.Add(1)

			return nil, want
		}),
	)
	require.Empty(t, cfg.ProducerID())

	writer, err := NewWriterReconnector(cfg)
	require.NoError(t, err)
	err = writer.WaitInit(context.Background())
	require.ErrorIs(t, err, want)
	require.NotNil(t, xerrors.RetryableError(err))
	require.EqualValues(t, 1, connects.Load())
}

func TestTransactionalProducerIDUsesAutomaticSeqNoByDefault(t *testing.T) {
	cfg := NewWriterReconnectorConfig(WithTransactionMode(), WithProducerID("producer"))
	require.Equal(t, "producer", cfg.ProducerID())
	require.True(t, cfg.AutoSetSeqNo)
	require.True(t, newWriterReconnectorStopped(cfg).needReceiveLastSeqNo())

	writer := newWriterReconnectorStopped(cfg)
	writer.onWriterChange(&SingleStreamWriter{LastSeqNumRequested: true, ReceivedLastSeqNum: 9})
	err := writer.Write(context.Background(), []PublicMessage{{Data: bytes.NewReader([]byte("message"))}})
	require.NoError(t, err)
	require.EqualValues(t, 10, writer.queue.messagesByOrder[1].SeqNo)
}

func TestTransactionalWriterRequiresSeqNoWhenAutomaticSeqNoDisabled(t *testing.T) {
	cfg := NewWriterReconnectorConfig(WithTransactionMode(), WithProducerID("producer"), WithAutoSetSeqNo(false))
	require.False(t, cfg.AutoSetSeqNo)
	require.False(t, newWriterReconnectorStopped(cfg).needReceiveLastSeqNo())

	writer := newWriterReconnectorStopped(cfg)
	writer.firstConnectionHandled.Store(true)
	err := writer.Write(context.Background(), []PublicMessage{{Data: bytes.NewReader([]byte("message"))}})
	require.ErrorIs(t, err, ErrNoSeqNo)
}

func TestTransactionalWriterWithoutProducerIDAssignsLocalSeqNo(t *testing.T) {
	cfg := NewWriterReconnectorConfig(WithTransactionMode())
	writer := newWriterReconnectorStopped(cfg)
	writer.firstConnectionHandled.Store(true)
	require.Empty(t, cfg.ProducerID())
	require.True(t, cfg.AutoSetSeqNo)
	require.False(t, writer.needReceiveLastSeqNo())

	for range 2 {
		err := writer.Write(context.Background(), []PublicMessage{{Data: bytes.NewReader([]byte("message"))}})
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, writer.queue.messagesByOrder[1].SeqNo)
	require.EqualValues(t, 2, writer.queue.messagesByOrder[2].SeqNo)
}
