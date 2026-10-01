package topicmultiwriter

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type countingSessionFactory struct {
	t       *testing.T
	created atomic.Int64
}

func (f *countingSessionFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	f.created.Add(1)

	return stubs.NewBasicWriter(f.t, cfg.OnAckReceivedCallback, cfg.AutoSetSeqNo, 0), nil
}

func TestMultiWriterWaitInitDoesNotOpenWriteSessions(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	factory := &countingSessionFactory{t: t}
	multiWriter := newTestMultiWriterWithCustomWritersFactory(
		t,
		func(context.Context, string) (topictypes.TopicDescription, error) {
			return stubs.DefaultStubTopicDescription(t), nil
		},
		factory,
		topicwriterinternal.WithAutoSetSeqNo(true),
	)

	require.NoError(t, multiWriter.WaitInit(ctx))
	require.Zero(t, factory.created.Load())
	require.NoError(t, multiWriter.Close(ctx))
}

type seqNoSessionFactory struct {
	mu         sync.Mutex
	baselines  map[int64]int64
	createdIDs []int64
	written    chan int64
}

func (f *seqNoSessionFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	partitionID, _ := cfg.PartitionID()
	f.mu.Lock()
	f.createdIDs = append(f.createdIDs, partitionID)
	baseline := f.baselines[partitionID]
	f.mu.Unlock()

	return &seqNoSessionWriter{baseline: baseline, written: f.written, ack: cfg.OnAckReceivedCallback}, nil
}

func (f *seqNoSessionFactory) created() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]int64(nil), f.createdIDs...)
}

type seqNoSessionWriter struct {
	baseline int64
	written  chan int64
	ack      func(int64)
}

func (w *seqNoSessionWriter) Close(context.Context) error { return nil }

func (w *seqNoSessionWriter) WaitInitInfo(context.Context) (topicwriterinternal.InitialInfo, error) {
	return topicwriterinternal.InitialInfo{LastSeqNum: w.baseline}, nil
}

func (w *seqNoSessionWriter) WriteInternal(_ context.Context, messages []topicwritercommon.MessageWithDataContent) error {
	for _, message := range messages {
		w.written <- message.SeqNo
		w.ack(message.SeqNo)
	}

	return nil
}

func TestMultiWriterAutoSeqNoUsesOpenedSessions(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	factory := &seqNoSessionFactory{
		baselines: map[int64]int64{1: 41, 2: 100, 3: 5},
		written:   make(chan int64, 3),
	}
	cfg := &MultiWriterConfig{}
	withWritersFactory(factory)(cfg)
	WithWriterPartitionByPartitionID()(cfg)
	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithAutoSetSeqNo(true)(writerCfg)
	topicwriterinternal.WithWaitAckOnWrite(true)(writerCfg)
	writer, err := NewMultiWriter(
		func(context.Context, string) (topictypes.TopicDescription, error) {
			return stubs.DefaultStubTopicDescription(t), nil
		},
		writerCfg,
		cfg,
	)
	require.NoError(t, err)
	require.NoError(t, writer.WaitInit(ctx))
	require.Empty(t, factory.created())

	for _, partitionID := range []int64{1, 2, 3} {
		require.NoError(t, writer.Write(ctx, []topicwriterinternal.PublicMessage{{
			PartitionID: partitionID,
			Data:        bytes.NewReader([]byte("message")),
		}}))
	}
	require.Equal(t, []int64{42, 101, 102}, []int64{
		<-factory.written, <-factory.written, <-factory.written,
	})
	require.Equal(t, []int64{1, 2, 3}, factory.created())
	require.NoError(t, writer.Close(ctx))
}
