package topicmultiwriter

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type lastSeqWritersFactory struct {
	writers map[int64]*orderedSeqWriter
}

func (f *lastSeqWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	partitionID, _ := cfg.PartitionID()
	w := f.writers[partitionID]
	if w == nil {
		w = &orderedSeqWriter{writes: make(chan int64, 1)}
	}
	w.onAckReceivedCallback = cfg.OnAckReceivedCallback

	return w, nil
}

func TestMultiWriterAutoSeqNoUsesOpenedSessionBaseline(t *testing.T) {
	ctx := xtest.Context(t)
	client := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
	factory := &lastSeqWritersFactory{writers: map[int64]*orderedSeqWriter{
		1: {lastSeqNo: 5, writes: make(chan int64, 2)},
		2: {lastSeqNo: 50, writes: make(chan int64, 1)},
	}}
	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(10)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)
	topicwriterinternal.WithAutoSetSeqNo(true)(writerCfg)
	multiCfg := MultiWriterConfig{}
	withWritersFactory(factory)(&multiCfg)
	WithProducerIDPrefix("test-producer")(&multiCfg)
	WithWriterPartitionByPartitionID()(&multiCfg)

	w, err := NewMultiWriter(
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return client.Describe(ctx, path)
		},
		writerCfg,
		&multiCfg,
	)
	require.NoError(t, err)
	require.NoError(t, w.WaitInit(ctx))
	require.Zero(t, w.getWritersCount(), "initialization must not open partition sessions")

	for _, tc := range []struct {
		partitionID int64
		wantSeqNo   int64
	}{
		{partitionID: 1, wantSeqNo: 6},
		{partitionID: 2, wantSeqNo: 51},
		{partitionID: 1, wantSeqNo: 52},
	} {
		require.NoError(t, w.Write(ctx, []topicwriterinternal.PublicMessage{{
			Data: bytes.NewReader([]byte("message")), PartitionID: tc.partitionID,
		}}))
		require.Equal(t, tc.wantSeqNo, <-factory.writers[tc.partitionID].writes)
	}
	require.EqualValues(t, 1, factory.writers[1].initCalls.Load())
	require.EqualValues(t, 1, factory.writers[2].initCalls.Load())

	require.NoError(t, w.Close(ctx))
}

func TestMultiWriterRechoosesPartitionSplitWhileMessageContentIsRead(t *testing.T) {
	ctx := xtest.Context(t)
	factory := &poolMockFactory{}
	chooser := partitionchooser.NewBoundPartitionChooser(partitionchooser.WithKeyHasher(func(key string) string {
		return key
	}))
	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithTopic("test/topic")(writerCfg)
	topicwriterinternal.WithMaxQueueLen(10)(writerCfg)
	topicwriterinternal.WithAutosetCreatedTime(false)(writerCfg)
	topicwriterinternal.WithAutoSetSeqNo(true)(writerCfg)
	multiCfg := MultiWriterConfig{}
	withWritersFactory(factory)(&multiCfg)
	WithProducerIDPrefix("test-producer")(&multiCfg)
	WithWriterPartitionByKey(chooser)(&multiCfg)

	w, err := NewMultiWriter(func(context.Context, string) (topictypes.TopicDescription, error) {
		return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
			{PartitionID: 0, Active: true, ToBound: []byte("m")},
			{PartitionID: 1, Active: true, FromBound: []byte("m")},
		}}, nil
	}, writerCfg, &multiCfg)
	require.NoError(t, err)
	require.NoError(t, w.WaitInit(ctx))
	t.Cleanup(func() {
		w.orchestrator.stop()
		_ = w.background.Close(context.Background(), nil)
	})

	reader := &blockingReader{started: make(chan struct{}), release: make(chan struct{}), data: []byte("message")}
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- w.Write(ctx, []topicwriterinternal.PublicMessage{{Data: reader, Key: "a"}})
	}()
	<-reader.started

	w.orchestrator.mu.WithLock(func() {
		parent := w.orchestrator.partitions[0]
		parent.ChildPartitionIDs = []int64{2, 3}
		children := []topictypes.PartitionInfo{
			{PartitionID: 2, Active: true, ToBound: []byte("g"), ParentPartitionIDs: []int64{0}},
			{PartitionID: 3, Active: true, FromBound: []byte("g"), ToBound: []byte("m"), ParentPartitionIDs: []int64{0}},
		}
		for _, child := range children {
			w.orchestrator.partitions[child.PartitionID] = &PartitionInfo{PartitionInfo: child}
		}
		require.NoError(t, chooser.AddNewPartitions(children...))
		chooser.RemovePartition(0)
	})
	probe, err := w.orchestrator.writerPool.get(0, false)
	require.NoError(t, err)
	close(reader.release)
	require.NoError(t, <-writeDone)
	require.False(t, probe.writer.(*poolTestWriter).closed.Load(), "the seqNo probe must stay open")
	w.orchestrator.mu.WithLock(func() {
		front := w.orchestrator.buf.inFlightMessages.Front()
		require.NotNil(t, front)
		require.Equal(t, int64(2), front.Value.PartitionID)
	})
}
