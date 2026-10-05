package topicmultiwriter

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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

	require.NoError(t, w.Close(ctx))
}
