package topicmultiwriter

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestMultiWriter_ResendsToActiveLeafAfterSuccessiveSplits(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	writes := make(chan successiveSplitWrite, 10)
	multiWriter := newSuccessiveSplitsMultiWriter(t, &successiveSplitWritersFactory{writes: writes})
	require.NoError(t, multiWriter.WaitInit(ctx))
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data:  bytes.NewBufferString("in-flight message"),
		Key:   "partition-key",
		SeqNo: 42,
	}}))
	require.Equal(t, int64(0), (<-writes).partitionID)

	// Both splits happened before the first topology refresh. The second
	// notification must not leave the message on the inactive intermediate node.
	require.NoError(t, multiWriter.orchestrator.onPartitionSplit(0))
	require.NoError(t, multiWriter.orchestrator.onPartitionSplit(2))
	var messagePartitionID int64
	multiWriter.orchestrator.mu.WithLock(func() {
		messagePartitionID = multiWriter.orchestrator.buf.inFlightMessages.Front().Value.PartitionID
	})
	require.Equal(t, int64(4), messagePartitionID, "in-flight message must move to the active leaf")

	resent := <-writes
	require.Equal(t, int64(4), resent.partitionID)
	require.Equal(t, int64(42), resent.seqNo)
	require.Equal(t, []byte("in-flight message"), resent.data)

	flushCtx, cancelFlush := context.WithCancel(ctx)
	cancelFlush()
	require.ErrorIs(t, multiWriter.Flush(flushCtx), context.Canceled)
	resent.ack(resent.seqNo)
	require.NoError(t, multiWriter.Flush(ctx))
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_RefreshesCommittedSeqNoAfterSuccessiveSplits(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	writes := make(chan successiveSplitWrite, 10)
	baselineQueries := make(chan string, 10)
	multiWriter := newSuccessiveSplitsMultiWriter(t, &successiveSplitWritersFactory{
		writes: writes,
		initInfo: func(_ context.Context, producerID string, pinned bool) (topicwriterinternal.InitialInfo, error) {
			if pinned {
				return topicwriterinternal.InitialInfo{LastSeqNum: 10}, nil
			}
			baselineQueries <- producerID

			return topicwriterinternal.InitialInfo{LastSeqNum: 42}, nil
		},
	})
	require.NoError(t, multiWriter.WaitInit(ctx))
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{
		{Data: bytes.NewBufferString("already committed"), Key: "partition-key", SeqNo: 42},
		{Data: bytes.NewBufferString("needs resend"), Key: "partition-key", SeqNo: 43},
	}))
	for _, seqNo := range []int64{42, 43} {
		written := <-writes
		require.Equal(t, int64(0), written.partitionID)
		require.Equal(t, seqNo, written.seqNo)
	}

	require.NoError(t, multiWriter.orchestrator.onPartitionSplit(0))
	var pendingSeqNos []int64
	multiWriter.orchestrator.mu.WithLock(func() {
		for iter := multiWriter.orchestrator.buf.inFlightMessages.Front(); iter != nil; iter = iter.Next() {
			pendingSeqNos = append(pendingSeqNos, iter.Value.SeqNo)
		}
	})
	require.Equal(t, []int64{43}, pendingSeqNos, "committed messages must not be resent")
	require.Len(t, baselineQueries, 1)
	require.Equal(t, "test-producer-0", <-baselineQueries)

	resent := <-writes
	require.Equal(t, int64(4), resent.partitionID)
	require.Equal(t, int64(43), resent.seqNo)
	require.Equal(t, []byte("needs resend"), resent.data)
	require.Empty(t, writes, "only the uncommitted message should be resent")
	resent.ack(resent.seqNo)
	require.NoError(t, multiWriter.Flush(ctx))
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_WaitsForAncestorBaselineBeforeWritingToDescendant(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	writes := make(chan successiveSplitWrite, 10)
	baselineStarted := make(chan string, 1)
	releaseBaseline := make(chan struct{})
	childInits := make(chan string, 10)
	multiWriter := newSuccessiveSplitsMultiWriter(t, &successiveSplitWritersFactory{
		writes: writes,
		initInfo: func(ctx context.Context, producerID string, pinned bool) (topicwriterinternal.InitialInfo, error) {
			if pinned {
				if producerID != "test-producer-0" {
					childInits <- producerID
				}

				return topicwriterinternal.InitialInfo{}, nil
			}
			baselineStarted <- producerID
			select {
			case <-releaseBaseline:
				return topicwriterinternal.InitialInfo{LastSeqNum: 41}, nil
			case <-ctx.Done():
				return topicwriterinternal.InitialInfo{}, ctx.Err()
			}
		},
	})
	require.NoError(t, multiWriter.WaitInit(ctx))
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewBufferString("needs resend"), Key: "partition-key", SeqNo: 42,
	}}))
	require.Equal(t, int64(0), (<-writes).partitionID)
	splitDone := make(chan error, 1)
	go func() {
		splitDone <- multiWriter.orchestrator.onPartitionSplit(0)
	}()
	require.Equal(t, "test-producer-0", <-baselineStarted)

	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewBufferString("new message"), Key: "partition-key", SeqNo: 43,
	}}))
	require.NoError(t, multiWriter.orchestrator.sender.step())
	require.Empty(t, childInits, "descendant session must wait for the ancestor baseline")
	require.Empty(t, writes, "descendant writes must wait for the ancestor baseline")

	close(releaseBaseline)
	require.NoError(t, <-splitDone)
	for _, seqNo := range []int64{42, 43} {
		written := <-writes
		require.Equal(t, int64(4), written.partitionID)
		require.Equal(t, seqNo, written.seqNo)
		written.ack(written.seqNo)
	}
	require.NoError(t, multiWriter.Flush(ctx))
	require.NoError(t, multiWriter.orchestrator.onPartitionSplit(0))
	require.NoError(t, multiWriter.orchestrator.onPartitionSplit(2))
	require.NoError(t, multiWriter.orchestrator.sender.step())
	require.Empty(t, writes, "repeated notifications must not resend acknowledged messages")
	require.NoError(t, multiWriter.Close(ctx))
}

func TestMultiWriter_AssignsSeqNoAfterSuccessiveSplits(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	writes := make(chan successiveSplitWrite, 1)
	multiWriter := newSuccessiveSplitsMultiWriter(t, &successiveSplitWritersFactory{
		writes: writes,
		initInfo: func(_ context.Context, _ string, pinned bool) (topicwriterinternal.InitialInfo, error) {
			if pinned {
				return topicwriterinternal.InitialInfo{}, nil
			}

			return topicwriterinternal.InitialInfo{LastSeqNum: 41}, nil
		},
	}, topicwriterinternal.WithAutoSetSeqNo(true))
	require.NoError(t, multiWriter.WaitInit(ctx))
	require.NoError(t, multiWriter.orchestrator.onPartitionSplit(0))
	require.NoError(t, multiWriter.Write(ctx, []topicwriterinternal.PublicMessage{{
		Data: bytes.NewBufferString("new message"), Key: "partition-key",
	}}))
	written := <-writes
	require.Equal(t, int64(4), written.partitionID)
	require.Equal(t, int64(42), written.seqNo)
	written.ack(written.seqNo)
	require.NoError(t, multiWriter.Flush(ctx))
	require.NoError(t, multiWriter.Close(ctx))
}

func newSuccessiveSplitsMultiWriter(
	t testing.TB,
	factory writersFactory,
	opts ...topicwriterinternal.PublicWriterOption,
) *MultiWriter {
	t.Helper()

	ctx := xtest.Context(t)
	describeCalls := 0
	multiWriter := newTestMultiWriterWithCustomWritersFactory(t,
		func(context.Context, string) (topictypes.TopicDescription, error) {
			describeCalls++
			if describeCalls == 1 {
				return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{{
					PartitionID: 0,
					Active:      true,
				}}}, nil
			}

			return successiveSplitsDescription(), nil
		},
		factory,
		opts...,
	)
	t.Cleanup(func() {
		multiWriter.orchestrator.stop()
		_ = multiWriter.background.Close(ctx, nil)
		_ = multiWriter.orchestrator.writerPool.close(ctx)
	})

	return multiWriter
}

func successiveSplitsDescription() topictypes.TopicDescription {
	return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{
		{PartitionID: 0, ChildPartitionIDs: []int64{2, 3}},
		{PartitionID: 2, ParentPartitionIDs: []int64{0}, ChildPartitionIDs: []int64{4, 5}, ToBound: []byte{0xff}},
		{PartitionID: 3, Active: true, ParentPartitionIDs: []int64{0}, FromBound: []byte{0xff}},
		{PartitionID: 4, Active: true, ParentPartitionIDs: []int64{2}, ToBound: []byte{0xfe}},
		{PartitionID: 5, Active: true, ParentPartitionIDs: []int64{2}, FromBound: []byte{0xfe}, ToBound: []byte{0xff}},
	}}
}

type successiveSplitWrite struct {
	partitionID int64
	seqNo       int64
	data        []byte
	ack         func(int64)
}

type successiveSplitWritersFactory struct {
	writes   chan successiveSplitWrite
	initInfo func(context.Context, string, bool) (topicwriterinternal.InitialInfo, error)
}

func (f *successiveSplitWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	partitionID, pinned := cfg.PartitionID()

	return &successiveSplitWriter{
		poolTestWriter: &poolTestWriter{},
		partitionID:    partitionID,
		writes:         f.writes,
		ack:            cfg.OnAckReceivedCallback,
		initInfo: func(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
			if f.initInfo == nil {
				return topicwriterinternal.InitialInfo{}, nil
			}

			return f.initInfo(ctx, cfg.ProducerID(), pinned)
		},
	}, nil
}

type successiveSplitWriter struct {
	*poolTestWriter

	partitionID int64
	writes      chan successiveSplitWrite
	ack         func(int64)
	initInfo    func(context.Context) (topicwriterinternal.InitialInfo, error)
}

func (w *successiveSplitWriter) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	return w.initInfo(ctx)
}

func (w *successiveSplitWriter) WriteInternal(
	ctx context.Context,
	messages []topicwritercommon.MessageWithDataContent,
) error {
	for i := range messages {
		data, err := messages[i].GetEncodedBytes(rawtopiccommon.CodecRaw)
		if err != nil {
			return err
		}
		w.writes <- successiveSplitWrite{
			partitionID: w.partitionID,
			seqNo:       messages[i].SeqNo,
			data:        bytes.Clone(data),
			ack:         w.ack,
		}
	}

	return nil
}
