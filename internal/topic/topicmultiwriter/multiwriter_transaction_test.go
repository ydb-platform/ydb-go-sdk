package topicmultiwriter

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// stubTopicTransaction is a minimal tx.Transaction for MultiWriterWithTransaction tests.
type stubTopicTransaction struct {
	tx.Identifier

	sessionID string
}

func newStubTopicTransaction(id string) *stubTopicTransaction {
	return &stubTopicTransaction{
		Identifier: tx.ID(id),
		sessionID:  "test-session",
	}
}

func (s *stubTopicTransaction) UnLazy(context.Context) error {
	return nil
}

func (s *stubTopicTransaction) SessionID() string {
	return s.sessionID
}

func (s *stubTopicTransaction) NodeID() uint32 {
	return 0
}

func (*stubTopicTransaction) OnBeforeCommit(tx.OnTransactionBeforeCommit) {}

func (*stubTopicTransaction) OnCompleted(tx.OnTransactionCompletedFunc) {}

func (*stubTopicTransaction) Rollback(context.Context) error {
	return nil
}

func TestMultiWriterWithTransaction_Write_SetsTx(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))

	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	require.NoError(t, multiWriter.WaitInit(ctx))

	stubTxn := newStubTopicTransaction("test-txn")

	wrapped := NewTopicMultiWriterTransaction(multiWriter, stubTxn, nil)

	messages := []topicwriterinternal.PublicMessage{
		{
			Data:  bytes.NewReader([]byte("a")),
			SeqNo: 1,
			Key:   "k1",
		},
		{
			Data:  bytes.NewReader([]byte("b")),
			SeqNo: 2,
			Key:   "k2",
		},
	}

	require.NoError(t, wrapped.Write(ctx, messages))

	for i := range messages {
		require.Same(t, stubTxn, messages[i].Tx, "message %d", i)
	}

	require.NoError(t, multiWriter.Close(ctx))
}

func TestTransactionalMultiWriter_DoesNotInitializeServerSeqNo(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	factory := &poolMockFactory{}
	multiWriterCfg := &MultiWriterConfig{}
	withWritersFactory(factory)(multiWriterCfg)
	WithWriterPartitionByKey(partitionchooser.NewBoundPartitionChooser())(multiWriterCfg)

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithAutoSetSeqNo(true)(writerCfg)
	source := partition.NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		return stubs.DefaultStubTopicDescription(t), nil
	}).Get(writerCfg.Topic())

	multiWriter, err := NewTransactionalMultiWriter(source, writerCfg, multiWriterCfg)
	require.NoError(t, err)
	require.NoError(t, multiWriter.WaitInit(ctx))
	require.Zero(t, factory.createCalls, "transaction initialization must not open seqNo sessions")
	require.NoError(t, multiWriter.Close(ctx))
}

func TestTransactionalMultiWriter_SynchronousWriteReturnsSessionError(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	sessionErr := errors.New("session failed")
	factory := &poolMockFactory{writeErr: sessionErr}
	multiWriterCfg := &MultiWriterConfig{}
	withWritersFactory(factory)(multiWriterCfg)
	WithWriterPartitionByKey(partitionchooser.NewBoundPartitionChooser())(multiWriterCfg)

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithAutoSetSeqNo(true)(writerCfg)
	topicwriterinternal.WithWaitAckOnWrite(true)(writerCfg)
	topicwriterinternal.WithMaxQueueLen(10)(writerCfg)
	source := partition.NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		return stubs.DefaultStubTopicDescription(t), nil
	}).Get(writerCfg.Topic())

	multiWriter, err := NewTransactionalMultiWriter(source, writerCfg, multiWriterCfg)
	require.NoError(t, err)
	writer := NewTopicMultiWriterTransaction(multiWriter, newStubTopicTransaction("test-txn"), nil)

	err = writer.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewReader([]byte("message")),
		Key:  "key",
	}})
	require.ErrorIs(t, err, sessionErr)
	require.ErrorIs(t, multiWriter.Close(ctx), sessionErr)
}

func TestTransactionalMultiWriter_AssignsIncreasingSessionSeqNo(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	partitionWriter := &orderedSeqWriter{writes: make(chan int64, 2)}
	multiWriterCfg := &MultiWriterConfig{}
	withWritersFactory(orderedSeqWriterFactory{writer: partitionWriter})(multiWriterCfg)
	WithWriterPartitionByPartitionID()(multiWriterCfg)

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithAutoSetSeqNo(true)(writerCfg)
	topicwriterinternal.WithWaitAckOnWrite(true)(writerCfg)
	topicwriterinternal.WithMaxQueueLen(10)(writerCfg)
	source := partition.NewSources(func(context.Context, string) (topictypes.TopicDescription, error) {
		return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{{
			PartitionID: 1,
			Active:      true,
		}}}, nil
	}).Get(writerCfg.Topic())

	multiWriter, err := NewTransactionalMultiWriter(source, writerCfg, multiWriterCfg)
	require.NoError(t, err)
	writer := NewTopicMultiWriterTransaction(multiWriter, newStubTopicTransaction("test-txn"), nil)
	require.NoError(t, writer.Write(ctx, []topicwriterinternal.PublicMessage{
		{Data: bytes.NewReader([]byte("first")), PartitionID: 1},
		{Data: bytes.NewReader([]byte("second")), PartitionID: 1},
	}))
	require.Equal(t, int64(1), <-partitionWriter.writes)
	require.Equal(t, int64(2), <-partitionWriter.writes)
	require.NoError(t, multiWriter.Close(ctx))
}
